package dispatcher

import (
	"context"
	"errors"
	"log/slog"
	"math"
	"math/rand"
	"net/http"
	"sync"
	"time"

	"github.com/nuetzliches/hookaido/internal/queue"
)

type RetryConfig struct {
	Type   string
	Max    int
	Base   time.Duration
	Cap    time.Duration
	Jitter float64
}

type HMACSigningConfig struct {
	SecretRef      string
	SecretVersions []HMACSigningSecretVersion

	SecretSelection string

	SignatureHeader string
	TimestampHeader string
}

type HMACSigningSecretVersion struct {
	ID string

	Ref string

	ValidFrom  time.Time
	ValidUntil time.Time
	HasUntil   bool
}

type TargetConfig struct {
	URL      string
	Timeout  time.Duration
	Retry    RetryConfig
	SignHMAC *HMACSigningConfig
}

type RouteConfig struct {
	Route       string
	Targets     []TargetConfig
	Concurrency int
}

type PushDispatcher struct {
	Store          queue.Store
	Deliverer      Deliverer
	Routes         []RouteConfig
	Logger         *slog.Logger
	MaxWait        time.Duration
	LeaseSlack     time.Duration
	ObserveAttempt func(outcome queue.AttemptOutcome)

	stopOnce sync.Once
	stopCh   chan struct{}
	wg       sync.WaitGroup
}

// Start spawns delivery goroutines. Call Drain to stop them gracefully.
func (d *PushDispatcher) Start() {
	if d.Deliverer == nil || d.Store == nil {
		return
	}
	logger := d.Logger
	if logger == nil {
		logger = slog.Default()
	}
	rand.Seed(time.Now().UnixNano())
	maxWait := d.MaxWait
	if maxWait <= 0 {
		maxWait = 2 * time.Second
	}
	leaseSlack := d.LeaseSlack
	if leaseSlack <= 0 {
		leaseSlack = 30 * time.Second
	}

	d.stopCh = make(chan struct{})

	for _, rt := range d.Routes {
		if len(rt.Targets) == 0 {
			continue
		}
		concurrency := rt.Concurrency
		if concurrency <= 0 {
			concurrency = 1
		}
		sem := make(chan struct{}, concurrency)

		for _, target := range rt.Targets {
			d.wg.Add(1)
			go d.runTarget(logger, rt.Route, target, sem, maxWait, leaseSlack)
		}
	}
}

// Drain signals all goroutines to stop accepting new work and waits for
// in-flight deliveries to complete. Returns true if all goroutines finished
// before the timeout, false if the timeout expired.
func (d *PushDispatcher) Drain(timeout time.Duration) bool {
	if d.stopCh == nil {
		return true
	}
	d.stopOnce.Do(func() { close(d.stopCh) })
	done := make(chan struct{})
	go func() {
		d.wg.Wait()
		close(done)
	}()
	select {
	case <-done:
		return true
	case <-time.After(timeout):
		return false
	}
}

func (d *PushDispatcher) runTarget(logger *slog.Logger, route string, target TargetConfig, sem chan struct{}, maxWait, leaseSlack time.Duration) {
	defer d.wg.Done()

	if logger == nil {
		logger = slog.Default()
	}

	timeout := target.Timeout
	if timeout <= 0 {
		timeout = 10 * time.Second
	}
	leaseTTL := timeout + leaseSlack
	if leaseTTL < 30*time.Second {
		leaseTTL = 30 * time.Second
	}

	for {
		select {
		case <-d.stopCh:
			return
		case sem <- struct{}{}:
		}

		resp, err := d.Store.Dequeue(queue.DequeueRequest{
			Route:    route,
			Target:   target.URL,
			Batch:    1,
			MaxWait:  maxWait,
			LeaseTTL: leaseTTL,
		})
		if err != nil {
			<-sem
			logger.Warn("dispatcher_dequeue_failed",
				slog.String("route", route),
				slog.String("target", target.URL),
				slog.Any("err", err),
			)
			time.Sleep(200 * time.Millisecond)
			continue
		}
		if len(resp.Items) == 0 {
			<-sem
			continue
		}

		env := resp.Items[0]
		d.handleDelivery(logger, env, target)
		<-sem
	}
}

func (d *PushDispatcher) handleDelivery(logger *slog.Logger, env queue.Envelope, target TargetConfig) {
	if logger == nil {
		logger = slog.Default()
	}

	header := make(http.Header, len(env.Headers))
	for k, v := range env.Headers {
		header.Set(k, v)
	}

	delivery := Delivery{
		ID:     env.ID,
		Target: env.Target,
		Method: http.MethodPost,
		URL:    target.URL,
		Header: header,
		Body:   env.Payload,
		Sign:   target.SignHMAC,
	}

	timeout := target.Timeout
	if timeout <= 0 {
		timeout = 10 * time.Second
	}
	reqCtx, cancel := context.WithTimeout(context.Background(), timeout)
	res := d.Deliverer.Deliver(reqCtx, delivery)
	cancel()

	attempt := queue.DeliveryAttempt{
		EventID:    env.ID,
		Route:      env.Route,
		Target:     target.URL,
		Attempt:    env.Attempt,
		StatusCode: res.StatusCode,
	}
	if res.Err != nil {
		attempt.Error = res.Err.Error()
	}

	if isSuccess(res) {
		attempt.Outcome = queue.AttemptOutcomeAcked
		d.recordAttempt(logger, attempt)
		if err := d.Store.Ack(env.LeaseID); err != nil && !errors.Is(err, queue.ErrLeaseExpired) {
			logger.Warn("dispatcher_ack_failed",
				slog.String("route", env.Route),
				slog.String("target", target.URL),
				slog.String("lease_id", env.LeaseID),
				slog.Any("err", err),
			)
		}
		return
	}

	shouldRetry := shouldRetry(res)
	if shouldRetry && env.Attempt < target.Retry.Max {
		delay := retryDelay(env.Attempt, target.Retry)
		attempt.Outcome = queue.AttemptOutcomeRetry
		d.recordAttempt(logger, attempt)
		if err := d.Store.Nack(env.LeaseID, delay); err != nil && !errors.Is(err, queue.ErrLeaseExpired) {
			logger.Warn("dispatcher_nack_failed",
				slog.String("route", env.Route),
				slog.String("target", target.URL),
				slog.String("lease_id", env.LeaseID),
				slog.Any("err", err),
			)
		}
		return
	}

	reason := "no_retry"
	if errors.Is(res.Err, ErrPolicyDenied) {
		reason = "policy_denied"
	} else if shouldRetry {
		reason = "max_retries"
	}
	attempt.Outcome = queue.AttemptOutcomeDead
	attempt.DeadReason = reason
	d.recordAttempt(logger, attempt)
	if err := d.Store.MarkDead(env.LeaseID, reason); err != nil && !errors.Is(err, queue.ErrLeaseExpired) {
		logger.Warn("dispatcher_mark_dead_failed",
			slog.String("route", env.Route),
			slog.String("target", target.URL),
			slog.String("lease_id", env.LeaseID),
			slog.Any("err", err),
		)
	}
}

func (d *PushDispatcher) recordAttempt(logger *slog.Logger, attempt queue.DeliveryAttempt) {
	if d.ObserveAttempt != nil {
		d.ObserveAttempt(attempt.Outcome)
	}
	if err := d.Store.RecordAttempt(attempt); err != nil {
		logger.Warn("dispatcher_record_attempt_failed",
			slog.String("route", attempt.Route),
			slog.String("target", attempt.Target),
			slog.String("event_id", attempt.EventID),
			slog.Int("attempt", attempt.Attempt),
			slog.Any("err", err),
		)
	}
}

func shouldRetry(res Result) bool {
	if res.Err != nil {
		if errors.Is(res.Err, ErrPolicyDenied) {
			return false
		}
		return true
	}
	code := res.StatusCode
	if code == http.StatusRequestTimeout || code == http.StatusTooManyRequests {
		return true
	}
	if code >= 500 {
		return true
	}
	return false
}

func isSuccess(res Result) bool {
	if res.Err != nil {
		return false
	}
	return res.StatusCode >= 200 && res.StatusCode < 300
}

func retryDelay(attempt int, retry RetryConfig) time.Duration {
	if retry.Base <= 0 {
		return 0
	}
	exp := float64(attempt - 1)
	base := float64(retry.Base)
	delay := base * math.Pow(2, exp)
	if retry.Cap > 0 {
		capVal := float64(retry.Cap)
		if delay > capVal {
			delay = capVal
		}
	}
	if retry.Jitter > 0 {
		j := retry.Jitter
		if j > 1 {
			j = 1
		}
		delta := (rand.Float64()*2 - 1) * j
		delay = delay * (1 + delta)
		if delay < 0 {
			delay = 0
		}
	}
	return time.Duration(delay)
}
