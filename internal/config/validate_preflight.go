package config

import (
	"fmt"
	"sort"
	"strings"

	"github.com/nuetzliches/hookaido/internal/secrets"
)

func validateSecretPreflight(compiled Compiled) []string {
	usages := map[string][]string{}

	for i, ref := range compiled.PullAPI.AuthTokens {
		addSecretRefUsage(usages, ref, fmt.Sprintf("pull_api.auth token[%d]", i))
	}
	for i, ref := range compiled.AdminAPI.AuthTokens {
		addSecretRefUsage(usages, ref, fmt.Sprintf("admin_api.auth token[%d]", i))
	}

	secretIDs := make([]string, 0, len(compiled.Secrets))
	for id := range compiled.Secrets {
		secretIDs = append(secretIDs, id)
	}
	sort.Strings(secretIDs)
	for _, id := range secretIDs {
		sc := compiled.Secrets[id]
		addSecretRefUsage(usages, sc.ValueRef, fmt.Sprintf("secret %q value", id))
	}

	for i, route := range compiled.Routes {
		routePath := strings.TrimSpace(route.Path)
		if routePath == "" {
			routePath = fmt.Sprintf("#%d", i)
		}

		if route.Pull != nil {
			for j, ref := range route.Pull.AuthTokens {
				addSecretRefUsage(usages, ref, fmt.Sprintf("route %q pull.auth token[%d]", routePath, j))
			}
		}

		for j, ref := range route.AuthHMACSecrets {
			addSecretRefUsage(usages, ref, fmt.Sprintf("route %q auth hmac secret[%d]", routePath, j))
		}

		for j, delivery := range route.Deliveries {
			signing := delivery.SigningHMAC
			if !signing.Enabled {
				continue
			}
			addSecretRefUsage(usages, signing.SecretRef, fmt.Sprintf("route %q deliver[%d] sign hmac", routePath, j))
			for k, version := range signing.SecretVersions {
				addSecretRefUsage(usages, version.ValueRef, fmt.Sprintf("route %q deliver[%d] sign hmac secret_ref[%d]", routePath, j, k))
			}
		}
	}

	refs := make([]string, 0, len(usages))
	for ref := range usages {
		refs = append(refs, ref)
	}
	sort.Strings(refs)

	errs := make([]string, 0)
	for _, ref := range refs {
		if _, err := secrets.LoadRef(ref); err != nil {
			contexts := uniqueSortedStrings(usages[ref])
			errs = append(errs, fmt.Sprintf("secret preflight %q used by %s: %v", ref, strings.Join(contexts, ", "), err))
		}
	}
	return errs
}

func addSecretRefUsage(usages map[string][]string, ref, usage string) {
	ref = strings.TrimSpace(ref)
	if ref == "" {
		return
	}
	usages[ref] = append(usages[ref], usage)
}

func uniqueSortedStrings(values []string) []string {
	if len(values) == 0 {
		return nil
	}
	out := append([]string(nil), values...)
	sort.Strings(out)
	w := 1
	for _, v := range out[1:] {
		if v == out[w-1] {
			continue
		}
		out[w] = v
		w++
	}
	return out[:w]
}
