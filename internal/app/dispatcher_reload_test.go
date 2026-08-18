package app

import (
	"context"
	"os"
	"syscall"
	"testing"
	"time"
)

// A delivery header is commonly an auth token, so a reload that rotates one has
// to swap the dispatcher. The target comparison ignored CustomHeaders, so the
// reload reported success, `running` and every observability surface showed the
// new value, and deliveries kept sending the old token until restart.
func TestDispatcherConfigEqual_DetectsHeaderAndExecEnvChanges(t *testing.T) {
	tests := []struct {
		name    string
		running string
		updated string
	}{
		{
			name: "delivery header value",
			running: `
"/x" {
  deliver "https://ci.internal/one" {
    header "Authorization" "Bearer OLD"
  }
}
`,
			updated: `
"/x" {
  deliver "https://ci.internal/one" {
    header "Authorization" "Bearer NEW"
  }
}
`,
		},
		{
			name: "delivery header added",
			running: `
"/x" {
  deliver "https://ci.internal/one" {}
}
`,
			updated: `
"/x" {
  deliver "https://ci.internal/one" {
    header "X-Env" "prod"
  }
}
`,
		},
		{
			name: "exec env value",
			running: `
"/x" {
  deliver exec "/opt/hooks/run.sh" {
    env "MODE" "prod"
  }
}
`,
			updated: `
"/x" {
  deliver exec "/opt/hooks/run.sh" {
    env "MODE" "staging"
  }
}
`,
		},
		{
			name: "http target replaced by exec target",
			running: `
"/x" {
  deliver "https://ci.internal/one" {}
}
`,
			updated: `
"/x" {
  deliver exec "/opt/hooks/run.sh" {}
}
`,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			running := compileForReloadTest(t, tc.running)
			updated := compileForReloadTest(t, tc.updated)

			if requiresRestartForReload(updated, running) {
				t.Fatal("expected a live reload, not a restart-required rejection")
			}
			if dispatcherConfigEqual(updated, running) {
				t.Fatal("dispatcher config compared equal, so the reload would never reach the running dispatcher")
			}
		})
	}
}

// The counterpart: a change the dispatcher does not care about must not cost a
// drain and swap.
func TestDispatcherConfigEqual_IgnoresNonDeliveryChanges(t *testing.T) {
	running := compileForReloadTest(t, `
defaults { max_body 2mb }
"/x" {
  deliver "https://ci.internal/one" {
    header "Authorization" "Bearer OLD"
  }
}
`)
	updated := compileForReloadTest(t, `
defaults { max_body 2mb }
"/x" {
  rate_limit { rps 10 burst 20 }
  deliver "https://ci.internal/one" {
    header "Authorization" "Bearer OLD"
  }
}
`)

	if !dispatcherConfigEqual(updated, running) {
		t.Fatal("a rate-limit change must not force a dispatcher swap")
	}
}

// SIGHUP is registered before the listeners come up, because its default
// disposition would otherwise kill the process. Handling it that early is a
// different matter: a reload that landed before the startup sequence installed
// the initial dispatcher started one for the updated config, which the startup
// path then overwrote — leaking a running, undrainable dispatcher while the
// retained one ran the stale config.
func TestRunReloadSignalHandler_WaitsForStartup(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	hup := make(chan os.Signal, 1)
	startupDone := make(chan struct{})
	reloads := make(chan string, 4)

	go runReloadSignalHandler(ctx, hup, startupDone, func(trigger string) { reloads <- trigger })

	// A signal inside the startup window must not be acted on...
	hup <- syscall.SIGHUP
	select {
	case trigger := <-reloads:
		t.Fatalf("reload ran during startup (trigger %q)", trigger)
	case <-time.After(100 * time.Millisecond):
	}

	// ...and must not be lost either.
	close(startupDone)
	select {
	case trigger := <-reloads:
		if trigger != "signal_sighup" {
			t.Fatalf("trigger = %q, want signal_sighup", trigger)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("the signal received during startup was dropped")
	}

	// Later signals are handled normally.
	hup <- syscall.SIGHUP
	select {
	case <-reloads:
	case <-time.After(2 * time.Second):
		t.Fatal("a signal after startup was not handled")
	}
}

func TestRunReloadSignalHandler_StopsOnContextCancel(t *testing.T) {
	for _, tc := range []struct {
		name         string
		closeStartup bool
	}{
		{name: "during startup", closeStartup: false},
		{name: "after startup", closeStartup: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			hup := make(chan os.Signal, 1)
			startupDone := make(chan struct{})
			if tc.closeStartup {
				close(startupDone)
			}

			done := make(chan struct{})
			go func() {
				runReloadSignalHandler(ctx, hup, startupDone, func(string) {
					t.Error("reload must not run after cancellation")
				})
				close(done)
			}()

			cancel()
			select {
			case <-done:
			case <-time.After(2 * time.Second):
				t.Fatal("handler did not exit on context cancellation")
			}
		})
	}
}
