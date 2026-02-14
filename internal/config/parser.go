package config

import (
	"errors"
	"fmt"
	"strings"
)

type parser struct {
	lex     *lexer
	peeked  token
	hasPeek bool
}

func newParser(src string) *parser {
	return &parser{lex: newLexer(src)}
}

func (p *parser) parse() (*Config, error) {
	cfg := &Config{}

	var sawStmt bool
	for {
		tok, err := p.peek()
		if err != nil {
			return nil, err
		}
		if tok.kind == tokEOF {
			break
		}
		if tok.kind == tokComment {
			_, _ = p.next()
			if !sawStmt {
				cfg.Preamble = append(cfg.Preamble, tok.text)
			}
			continue
		}

		sawStmt = true
		switch tok.kind {
		case tokIdent:
			if strings.HasPrefix(tok.text, "/") {
				route, err := p.parseRouteBlock()
				if err != nil {
					return nil, err
				}
				cfg.Routes = append(cfg.Routes, route)
				continue
			}
			if strings.HasPrefix(tok.text, "@") {
				m, err := p.parseNamedMatcherBlock()
				if err != nil {
					return nil, err
				}
				for _, existing := range cfg.NamedMatchers {
					if existing.Name == m.Name {
						return nil, p.errAt(tok.pos, "duplicate matcher %q", "@"+m.Name)
					}
				}
				cfg.NamedMatchers = append(cfg.NamedMatchers, m)
				continue
			}
			if err := p.parseTopLevelBlock(cfg); err != nil {
				return nil, err
			}
		case tokString:
			route, err := p.parseRouteBlock()
			if err != nil {
				return nil, err
			}
			cfg.Routes = append(cfg.Routes, route)
		default:
			return nil, p.errAt(tok.pos, "unexpected token %q", tok.text)
		}
	}

	if !sawStmt {
		return nil, nil
	}
	return cfg, nil
}

func (p *parser) parseTopLevelBlock(cfg *Config) error {
	nameTok, _ := p.next()
	if nameTok.kind != tokIdent {
		return p.errAt(nameTok.pos, "expected identifier")
	}

	switch nameTok.text {
	case "ingress":
		if cfg.Ingress != nil {
			return p.errAt(nameTok.pos, "duplicate ingress block")
		}
		b, err := p.parseIngressBlock()
		if err != nil {
			return err
		}
		cfg.Ingress = b
		return nil
	case "defaults":
		if cfg.Defaults != nil {
			return p.errAt(nameTok.pos, "duplicate defaults block")
		}
		b, err := p.parseDefaultsBlock()
		if err != nil {
			return err
		}
		cfg.Defaults = b
		return nil
	case "vars":
		if cfg.Vars != nil {
			return p.errAt(nameTok.pos, "duplicate vars block")
		}
		b, err := p.parseVarsBlock()
		if err != nil {
			return err
		}
		cfg.Vars = b
		return nil
	case "secrets":
		if cfg.Secrets != nil {
			return p.errAt(nameTok.pos, "duplicate secrets block")
		}
		b, err := p.parseSecretsBlock()
		if err != nil {
			return err
		}
		cfg.Secrets = b
		return nil
	case "pull_api":
		if cfg.PullAPI != nil {
			return p.errAt(nameTok.pos, "duplicate pull_api block")
		}
		b, err := p.parseAPIBlock("pull_api")
		if err != nil {
			return err
		}
		cfg.PullAPI = b
		return nil
	case "admin_api":
		if cfg.AdminAPI != nil {
			return p.errAt(nameTok.pos, "duplicate admin_api block")
		}
		b, err := p.parseAPIBlock("admin_api")
		if err != nil {
			return err
		}
		cfg.AdminAPI = b
		return nil
	case "observability":
		if cfg.Observability != nil {
			return p.errAt(nameTok.pos, "duplicate observability block")
		}
		b, err := p.parseObservabilityBlock()
		if err != nil {
			return err
		}
		cfg.Observability = b
		return nil
	case "queue_retention":
		if cfg.QueueRetention != nil {
			return p.errAt(nameTok.pos, "duplicate queue_retention block")
		}
		b, err := p.parseQueueRetentionBlock()
		if err != nil {
			return err
		}
		cfg.QueueRetention = b
		return nil
	case "delivered_retention":
		if cfg.DeliveredRetention != nil {
			return p.errAt(nameTok.pos, "duplicate delivered_retention block")
		}
		b, err := p.parseDeliveredRetentionBlock()
		if err != nil {
			return err
		}
		cfg.DeliveredRetention = b
		return nil
	case "dlq_retention":
		if cfg.DLQRetention != nil {
			return p.errAt(nameTok.pos, "duplicate dlq_retention block")
		}
		b, err := p.parseDLQRetentionBlock()
		if err != nil {
			return err
		}
		cfg.DLQRetention = b
		return nil
	case "queue_limits":
		if cfg.QueueLimits != nil {
			return p.errAt(nameTok.pos, "duplicate queue_limits block")
		}
		b, err := p.parseQueueLimitsBlock()
		if err != nil {
			return err
		}
		cfg.QueueLimits = b
		return nil
	case "inbound", "outbound", "internal":
		ct := ChannelType(nameTok.text)
		next, err := p.peek()
		if err != nil {
			return err
		}
		if next.kind == tokLBrace {
			// Wrapper form: channel_type { routes... }
			_, _ = p.next() // consume {
			for {
				tok, err := p.peek()
				if err != nil {
					return err
				}
				if tok.kind == tokRBrace {
					_, _ = p.next()
					break
				}
				if tok.kind == tokEOF {
					return p.errAt(tok.pos, "unexpected EOF in %s block (missing '}')", nameTok.text)
				}
				if tok.kind == tokComment {
					_, _ = p.next()
					continue
				}
				route, err := p.parseRouteBlock()
				if err != nil {
					return err
				}
				route.ChannelType = ct
				cfg.Routes = append(cfg.Routes, route)
			}
		} else {
			// Single-route shorthand: channel_type /path { ... }
			route, err := p.parseRouteBlock()
			if err != nil {
				return err
			}
			route.ChannelType = ct
			cfg.Routes = append(cfg.Routes, route)
		}
		return nil
	default:
		return p.errAt(nameTok.pos, "unknown top-level block %q", nameTok.text)
	}
}

func (p *parser) parseIngressBlock() (*IngressBlock, error) {
	if _, err := p.expect(tokLBrace, "expected '{' after ingress"); err != nil {
		return nil, err
	}
	out := &IngressBlock{}
	listenSet := false

	for {
		tok, err := p.peek()
		if err != nil {
			return nil, err
		}
		if tok.kind == tokEOF {
			return nil, p.errAt(tok.pos, "unexpected EOF (missing '}')")
		}
		if tok.kind == tokRBrace {
			_, _ = p.next()
			break
		}
		if tok.kind == tokComment {
			_, _ = p.next()
			continue
		}

		dirTok, _ := p.next()
		if dirTok.kind != tokIdent {
			return nil, p.errAt(dirTok.pos, "expected directive name")
		}
		switch dirTok.text {
		case "listen":
			if listenSet {
				return nil, p.errAt(dirTok.pos, "duplicate ingress listen")
			}
			v, quoted, err := p.parseValue()
			if err != nil {
				return nil, err
			}
			out.Listen = v
			out.ListenQuoted = quoted
			listenSet = true
		case "tls":
			if out.TLS != nil {
				return nil, p.errAt(dirTok.pos, "duplicate ingress tls block")
			}
			tls, err := p.parseTLSBlock()
			if err != nil {
				return nil, err
			}
			out.TLS = tls
		case "rate_limit":
			if out.RateLimit != nil {
				return nil, p.errAt(dirTok.pos, "duplicate ingress rate_limit block")
			}
			rl, err := p.parseRateLimitBlock()
			if err != nil {
				return nil, err
			}
			out.RateLimit = rl
		default:
			return nil, p.errAt(dirTok.pos, "unknown ingress directive %q", dirTok.text)
		}
	}

	return out, nil
}

func (p *parser) parseDefaultsBlock() (*DefaultsBlock, error) {
	if _, err := p.expect(tokLBrace, "expected '{' after defaults"); err != nil {
		return nil, err
	}
	out := &DefaultsBlock{}

	for {
		tok, err := p.peek()
		if err != nil {
			return nil, err
		}
		if tok.kind == tokEOF {
			return nil, p.errAt(tok.pos, "unexpected EOF (missing '}')")
		}
		if tok.kind == tokRBrace {
			_, _ = p.next()
			break
		}
		if tok.kind == tokComment {
			_, _ = p.next()
			continue
		}

		dirTok, _ := p.next()
		if dirTok.kind != tokIdent {
			return nil, p.errAt(dirTok.pos, "expected directive name")
		}
		switch dirTok.text {
		case "max_body":
			if out.MaxBodySet {
				return nil, p.errAt(dirTok.pos, "duplicate defaults max_body")
			}
			v, quoted, err := p.parseValue()
			if err != nil {
				return nil, err
			}
			out.MaxBody = v
			out.MaxBodyQuoted = quoted
			out.MaxBodySet = true
		case "max_headers":
			if out.MaxHeadersSet {
				return nil, p.errAt(dirTok.pos, "duplicate defaults max_headers")
			}
			v, quoted, err := p.parseValue()
			if err != nil {
				return nil, err
			}
			out.MaxHeaders = v
			out.MaxHeadersQuoted = quoted
			out.MaxHeadersSet = true
		case "egress":
			if out.Egress != nil {
				return nil, p.errAt(dirTok.pos, "duplicate egress block")
			}
			eg, err := p.parseEgressBlock()
			if err != nil {
				return nil, err
			}
			out.Egress = eg
		case "publish_policy":
			if out.PublishPolicy != nil {
				return nil, p.errAt(dirTok.pos, "duplicate publish_policy block")
			}
			pp, err := p.parsePublishPolicyBlock()
			if err != nil {
				return nil, err
			}
			out.PublishPolicy = pp
		case "deliver":
			if out.Deliver != nil {
				return nil, p.errAt(dirTok.pos, "duplicate deliver block")
			}
			d, err := p.parseDefaultsDeliverBlock()
			if err != nil {
				return nil, err
			}
			out.Deliver = d
		case "trend_signals":
			if out.TrendSignals != nil {
				return nil, p.errAt(dirTok.pos, "duplicate trend_signals block")
			}
			ts, err := p.parseTrendSignalsBlock()
			if err != nil {
				return nil, err
			}
			out.TrendSignals = ts
		case "adaptive_backpressure":
			if out.AdaptiveBackpressure != nil {
				return nil, p.errAt(dirTok.pos, "duplicate adaptive_backpressure block")
			}
			ab, err := p.parseAdaptiveBackpressureBlock()
			if err != nil {
				return nil, err
			}
			out.AdaptiveBackpressure = ab
		default:
			return nil, p.errAt(dirTok.pos, "unknown defaults directive %q", dirTok.text)
		}
	}

	return out, nil
}

func isDefaultsDirective(name string) bool {
	switch strings.TrimSpace(name) {
	case "max_body",
		"max_headers",
		"egress",
		"publish_policy",
		"deliver",
		"trend_signals",
		"adaptive_backpressure":
		return true
	default:
		return false
	}
}

func (p *parser) parseVarsBlock() (*VarsBlock, error) {
	if _, err := p.expect(tokLBrace, "expected '{' after vars"); err != nil {
		return nil, err
	}
	out := &VarsBlock{}

	for {
		tok, err := p.peek()
		if err != nil {
			return nil, err
		}
		if tok.kind == tokEOF {
			return nil, p.errAt(tok.pos, "unexpected EOF (missing '}')")
		}
		if tok.kind == tokRBrace {
			_, _ = p.next()
			break
		}
		if tok.kind == tokComment {
			_, _ = p.next()
			continue
		}

		name, nameQuoted, err := p.parseValue()
		if err != nil {
			return nil, err
		}
		value, valueQuoted, err := p.parseValue()
		if err != nil {
			return nil, err
		}

		out.Items = append(out.Items, VarItem{
			Name:        name,
			NameQuoted:  nameQuoted,
			Value:       value,
			ValueQuoted: valueQuoted,
		})
	}

	return out, nil
}

func (p *parser) parseDefaultsDeliverBlock() (*DeliverBlock, error) {
	if _, err := p.expect(tokLBrace, "expected '{' after deliver"); err != nil {
		return nil, err
	}
	out := &DeliverBlock{}

	for {
		tok, err := p.peek()
		if err != nil {
			return nil, err
		}
		if tok.kind == tokEOF {
			return nil, p.errAt(tok.pos, "unexpected EOF (missing '}')")
		}
		if tok.kind == tokRBrace {
			_, _ = p.next()
			break
		}
		if tok.kind == tokComment {
			_, _ = p.next()
			continue
		}

		dirTok, _ := p.next()
		if dirTok.kind != tokIdent {
			return nil, p.errAt(dirTok.pos, "expected directive name")
		}
		switch dirTok.text {
		case "retry":
			if out.Retry != nil {
				return nil, p.errAt(dirTok.pos, "duplicate retry directive")
			}
			r, err := p.parseRetryDirective()
			if err != nil {
				return nil, err
			}
			out.Retry = r
		case "timeout":
			if out.TimeoutSet {
				return nil, p.errAt(dirTok.pos, "duplicate timeout directive")
			}
			v, quoted, err := p.parseValue()
			if err != nil {
				return nil, err
			}
			out.Timeout = v
			out.TimeoutQuoted = quoted
			out.TimeoutSet = true
		case "concurrency":
			if out.ConcurrencySet {
				return nil, p.errAt(dirTok.pos, "duplicate concurrency directive")
			}
			v, quoted, err := p.parseValue()
			if err != nil {
				return nil, err
			}
			out.Concurrency = v
			out.ConcurrencyQuoted = quoted
			out.ConcurrencySet = true
		default:
			return nil, p.errAt(dirTok.pos, "unknown deliver directive %q", dirTok.text)
		}
	}

	return out, nil
}

func (p *parser) parseTrendSignalsBlock() (*TrendSignalsBlock, error) {
	if _, err := p.expect(tokLBrace, "expected '{' after trend_signals"); err != nil {
		return nil, err
	}
	out := &TrendSignalsBlock{}

	for {
		tok, err := p.peek()
		if err != nil {
			return nil, err
		}
		if tok.kind == tokEOF {
			return nil, p.errAt(tok.pos, "unexpected EOF (missing '}')")
		}
		if tok.kind == tokRBrace {
			_, _ = p.next()
			break
		}
		if tok.kind == tokComment {
			_, _ = p.next()
			continue
		}

		dirTok, _ := p.next()
		if dirTok.kind != tokIdent {
			return nil, p.errAt(dirTok.pos, "expected directive name")
		}
		v, quoted, err := p.parseValue()
		if err != nil {
			return nil, err
		}

		switch dirTok.text {
		case "window":
			if out.WindowSet {
				return nil, p.errAt(dirTok.pos, "duplicate trend_signals window")
			}
			out.Window = v
			out.WindowQuoted = quoted
			out.WindowSet = true
		case "expected_capture_interval":
			if out.ExpectedCaptureIntervalSet {
				return nil, p.errAt(dirTok.pos, "duplicate trend_signals expected_capture_interval")
			}
			out.ExpectedCaptureInterval = v
			out.ExpectedCaptureIntervalQuoted = quoted
			out.ExpectedCaptureIntervalSet = true
		case "stale_grace_factor":
			if out.StaleGraceFactorSet {
				return nil, p.errAt(dirTok.pos, "duplicate trend_signals stale_grace_factor")
			}
			out.StaleGraceFactor = v
			out.StaleGraceFactorQuoted = quoted
			out.StaleGraceFactorSet = true
		case "sustained_growth_consecutive":
			if out.SustainedGrowthConsecutiveSet {
				return nil, p.errAt(dirTok.pos, "duplicate trend_signals sustained_growth_consecutive")
			}
			out.SustainedGrowthConsecutive = v
			out.SustainedGrowthConsecutiveQuoted = quoted
			out.SustainedGrowthConsecutiveSet = true
		case "sustained_growth_min_samples":
			if out.SustainedGrowthMinSamplesSet {
				return nil, p.errAt(dirTok.pos, "duplicate trend_signals sustained_growth_min_samples")
			}
			out.SustainedGrowthMinSamples = v
			out.SustainedGrowthMinSamplesQuoted = quoted
			out.SustainedGrowthMinSamplesSet = true
		case "sustained_growth_min_delta":
			if out.SustainedGrowthMinDeltaSet {
				return nil, p.errAt(dirTok.pos, "duplicate trend_signals sustained_growth_min_delta")
			}
			out.SustainedGrowthMinDelta = v
			out.SustainedGrowthMinDeltaQuoted = quoted
			out.SustainedGrowthMinDeltaSet = true
		case "recent_surge_min_total":
			if out.RecentSurgeMinTotalSet {
				return nil, p.errAt(dirTok.pos, "duplicate trend_signals recent_surge_min_total")
			}
			out.RecentSurgeMinTotal = v
			out.RecentSurgeMinTotalQuoted = quoted
			out.RecentSurgeMinTotalSet = true
		case "recent_surge_min_delta":
			if out.RecentSurgeMinDeltaSet {
				return nil, p.errAt(dirTok.pos, "duplicate trend_signals recent_surge_min_delta")
			}
			out.RecentSurgeMinDelta = v
			out.RecentSurgeMinDeltaQuoted = quoted
			out.RecentSurgeMinDeltaSet = true
		case "recent_surge_percent":
			if out.RecentSurgePercentSet {
				return nil, p.errAt(dirTok.pos, "duplicate trend_signals recent_surge_percent")
			}
			out.RecentSurgePercent = v
			out.RecentSurgePercentQuoted = quoted
			out.RecentSurgePercentSet = true
		case "dead_share_high_min_total":
			if out.DeadShareHighMinTotalSet {
				return nil, p.errAt(dirTok.pos, "duplicate trend_signals dead_share_high_min_total")
			}
			out.DeadShareHighMinTotal = v
			out.DeadShareHighMinTotalQuoted = quoted
			out.DeadShareHighMinTotalSet = true
		case "dead_share_high_percent":
			if out.DeadShareHighPercentSet {
				return nil, p.errAt(dirTok.pos, "duplicate trend_signals dead_share_high_percent")
			}
			out.DeadShareHighPercent = v
			out.DeadShareHighPercentQuoted = quoted
			out.DeadShareHighPercentSet = true
		case "queued_pressure_min_total":
			if out.QueuedPressureMinTotalSet {
				return nil, p.errAt(dirTok.pos, "duplicate trend_signals queued_pressure_min_total")
			}
			out.QueuedPressureMinTotal = v
			out.QueuedPressureMinTotalQuoted = quoted
			out.QueuedPressureMinTotalSet = true
		case "queued_pressure_percent":
			if out.QueuedPressurePercentSet {
				return nil, p.errAt(dirTok.pos, "duplicate trend_signals queued_pressure_percent")
			}
			out.QueuedPressurePercent = v
			out.QueuedPressurePercentQuoted = quoted
			out.QueuedPressurePercentSet = true
		case "queued_pressure_leased_multiplier":
			if out.QueuedPressureLeasedMultiplierSet {
				return nil, p.errAt(dirTok.pos, "duplicate trend_signals queued_pressure_leased_multiplier")
			}
			out.QueuedPressureLeasedMultiplier = v
			out.QueuedPressureLeasedMultiplierQuoted = quoted
			out.QueuedPressureLeasedMultiplierSet = true
		default:
			return nil, p.errAt(dirTok.pos, "unknown trend_signals directive %q", dirTok.text)
		}
	}

	return out, nil
}

func (p *parser) parseAdaptiveBackpressureBlock() (*AdaptiveBackpressureBlock, error) {
	if _, err := p.expect(tokLBrace, "expected '{' after adaptive_backpressure"); err != nil {
		return nil, err
	}
	out := &AdaptiveBackpressureBlock{}

	for {
		tok, err := p.peek()
		if err != nil {
			return nil, err
		}
		if tok.kind == tokEOF {
			return nil, p.errAt(tok.pos, "unexpected EOF (missing '}')")
		}
		if tok.kind == tokRBrace {
			_, _ = p.next()
			break
		}
		if tok.kind == tokComment {
			_, _ = p.next()
			continue
		}

		dirTok, _ := p.next()
		if dirTok.kind != tokIdent {
			return nil, p.errAt(dirTok.pos, "expected directive name")
		}

		v, quoted, err := p.parseValue()
		if err != nil {
			return nil, err
		}

		switch dirTok.text {
		case "enabled":
			if out.EnabledSet {
				return nil, p.errAt(dirTok.pos, "duplicate adaptive_backpressure enabled")
			}
			out.Enabled = v
			out.EnabledQuoted = quoted
			out.EnabledSet = true
		case "min_total":
			if out.MinTotalSet {
				return nil, p.errAt(dirTok.pos, "duplicate adaptive_backpressure min_total")
			}
			out.MinTotal = v
			out.MinTotalQuoted = quoted
			out.MinTotalSet = true
		case "queued_percent":
			if out.QueuedPercentSet {
				return nil, p.errAt(dirTok.pos, "duplicate adaptive_backpressure queued_percent")
			}
			out.QueuedPercent = v
			out.QueuedPercentQuoted = quoted
			out.QueuedPercentSet = true
		case "ready_lag":
			if out.ReadyLagSet {
				return nil, p.errAt(dirTok.pos, "duplicate adaptive_backpressure ready_lag")
			}
			out.ReadyLag = v
			out.ReadyLagQuoted = quoted
			out.ReadyLagSet = true
		case "oldest_queued_age":
			if out.OldestQueuedAgeSet {
				return nil, p.errAt(dirTok.pos, "duplicate adaptive_backpressure oldest_queued_age")
			}
			out.OldestQueuedAge = v
			out.OldestQueuedAgeQuoted = quoted
			out.OldestQueuedAgeSet = true
		case "sustained_growth":
			if out.SustainedGrowthSet {
				return nil, p.errAt(dirTok.pos, "duplicate adaptive_backpressure sustained_growth")
			}
			out.SustainedGrowth = v
			out.SustainedGrowthQuoted = quoted
			out.SustainedGrowthSet = true
		default:
			return nil, p.errAt(dirTok.pos, "unknown adaptive_backpressure directive %q", dirTok.text)
		}
	}

	return out, nil
}

func (p *parser) parseSecretsBlock() (*SecretsBlock, error) {
	if _, err := p.expect(tokLBrace, "expected '{' after secrets"); err != nil {
		return nil, err
	}
	out := &SecretsBlock{}

	for {
		tok, err := p.peek()
		if err != nil {
			return nil, err
		}
		if tok.kind == tokEOF {
			return nil, p.errAt(tok.pos, "unexpected EOF (missing '}')")
		}
		if tok.kind == tokRBrace {
			_, _ = p.next()
			break
		}
		if tok.kind == tokComment {
			_, _ = p.next()
			continue
		}

		dirTok, _ := p.next()
		if dirTok.kind != tokIdent || dirTok.text != "secret" {
			return nil, p.errAt(dirTok.pos, "expected secret directive")
		}
		id, quoted, err := p.parseValue()
		if err != nil {
			return nil, err
		}
		secret, err := p.parseSecretBlock(id, quoted)
		if err != nil {
			return nil, err
		}
		out.Items = append(out.Items, secret)
	}

	return out, nil
}

func (p *parser) parseSecretBlock(id string, idQuoted bool) (SecretBlock, error) {
	if _, err := p.expect(tokLBrace, "expected '{' after secret %q", id); err != nil {
		return SecretBlock{}, err
	}
	out := SecretBlock{ID: id, IDQuoted: idQuoted}

	for {
		tok, err := p.peek()
		if err != nil {
			return SecretBlock{}, err
		}
		if tok.kind == tokEOF {
			return SecretBlock{}, p.errAt(tok.pos, "unexpected EOF (missing '}')")
		}
		if tok.kind == tokRBrace {
			_, _ = p.next()
			break
		}
		if tok.kind == tokComment {
			_, _ = p.next()
			continue
		}

		dirTok, _ := p.next()
		if dirTok.kind != tokIdent {
			return SecretBlock{}, p.errAt(dirTok.pos, "expected directive name")
		}
		switch dirTok.text {
		case "value":
			if out.ValueSet {
				return SecretBlock{}, p.errAt(dirTok.pos, "duplicate secret value")
			}
			v, quoted, err := p.parseValue()
			if err != nil {
				return SecretBlock{}, err
			}
			out.Value = v
			out.ValueQuoted = quoted
			out.ValueSet = true
		case "valid_from":
			if out.ValidFromSet {
				return SecretBlock{}, p.errAt(dirTok.pos, "duplicate secret valid_from")
			}
			v, quoted, err := p.parseValue()
			if err != nil {
				return SecretBlock{}, err
			}
			out.ValidFrom = v
			out.ValidFromQuoted = quoted
			out.ValidFromSet = true
		case "valid_until":
			if out.ValidUntilSet {
				return SecretBlock{}, p.errAt(dirTok.pos, "duplicate secret valid_until")
			}
			v, quoted, err := p.parseValue()
			if err != nil {
				return SecretBlock{}, err
			}
			out.ValidUntil = v
			out.ValidUntilQuoted = quoted
			out.ValidUntilSet = true
		default:
			return SecretBlock{}, p.errAt(dirTok.pos, "unknown secret directive %q", dirTok.text)
		}
	}

	return out, nil
}

func (p *parser) parseAPIBlock(name string) (*APIBlock, error) {
	if _, err := p.expect(tokLBrace, "expected '{' after %s", name); err != nil {
		return nil, err
	}
	out := &APIBlock{}
	listenSet := false
	prefixSet := false

	for {
		tok, err := p.peek()
		if err != nil {
			return nil, err
		}
		if tok.kind == tokEOF {
			return nil, p.errAt(tok.pos, "unexpected EOF (missing '}')")
		}
		if tok.kind == tokRBrace {
			_, _ = p.next()
			break
		}
		if tok.kind == tokComment {
			_, _ = p.next()
			continue
		}

		dirTok, _ := p.next()
		if dirTok.kind != tokIdent {
			return nil, p.errAt(dirTok.pos, "expected directive name")
		}
		switch dirTok.text {
		case "listen":
			if listenSet {
				return nil, p.errAt(dirTok.pos, "duplicate %s listen", name)
			}
			v, quoted, err := p.parseValue()
			if err != nil {
				return nil, err
			}
			out.Listen = v
			out.ListenQuoted = quoted
			listenSet = true
		case "prefix":
			if prefixSet {
				return nil, p.errAt(dirTok.pos, "duplicate %s prefix", name)
			}
			v, quoted, err := p.parseValue()
			if err != nil {
				return nil, err
			}
			out.Prefix = v
			out.PrefixQuoted = quoted
			prefixSet = true
		case "max_batch":
			if name != "pull_api" {
				return nil, p.errAt(dirTok.pos, "unknown %s directive %q", name, dirTok.text)
			}
			if out.MaxBatchSet {
				return nil, p.errAt(dirTok.pos, "duplicate %s max_batch", name)
			}
			v, quoted, err := p.parseValue()
			if err != nil {
				return nil, err
			}
			out.MaxBatch = v
			out.MaxBatchQuoted = quoted
			out.MaxBatchSet = true
		case "grpc_listen":
			if name != "pull_api" {
				return nil, p.errAt(dirTok.pos, "unknown %s directive %q", name, dirTok.text)
			}
			if out.GRPCListenSet {
				return nil, p.errAt(dirTok.pos, "duplicate %s grpc_listen", name)
			}
			v, quoted, err := p.parseValue()
			if err != nil {
				return nil, err
			}
			out.GRPCListen = v
			out.GRPCListenQuoted = quoted
			out.GRPCListenSet = true
		case "default_lease_ttl":
			if name != "pull_api" {
				return nil, p.errAt(dirTok.pos, "unknown %s directive %q", name, dirTok.text)
			}
			if out.DefaultLeaseTTLSet {
				return nil, p.errAt(dirTok.pos, "duplicate %s default_lease_ttl", name)
			}
			v, quoted, err := p.parseValue()
			if err != nil {
				return nil, err
			}
			out.DefaultLeaseTTL = v
			out.DefaultLeaseTTLQuoted = quoted
			out.DefaultLeaseTTLSet = true
		case "max_lease_ttl":
			if name != "pull_api" {
				return nil, p.errAt(dirTok.pos, "unknown %s directive %q", name, dirTok.text)
			}
			if out.MaxLeaseTTLSet {
				return nil, p.errAt(dirTok.pos, "duplicate %s max_lease_ttl", name)
			}
			v, quoted, err := p.parseValue()
			if err != nil {
				return nil, err
			}
			out.MaxLeaseTTL = v
			out.MaxLeaseTTLQuoted = quoted
			out.MaxLeaseTTLSet = true
		case "default_max_wait":
			if name != "pull_api" {
				return nil, p.errAt(dirTok.pos, "unknown %s directive %q", name, dirTok.text)
			}
			if out.DefaultMaxWaitSet {
				return nil, p.errAt(dirTok.pos, "duplicate %s default_max_wait", name)
			}
			v, quoted, err := p.parseValue()
			if err != nil {
				return nil, err
			}
			out.DefaultMaxWait = v
			out.DefaultMaxWaitQuoted = quoted
			out.DefaultMaxWaitSet = true
		case "max_wait":
			if name != "pull_api" {
				return nil, p.errAt(dirTok.pos, "unknown %s directive %q", name, dirTok.text)
			}
			if out.MaxWaitSet {
				return nil, p.errAt(dirTok.pos, "duplicate %s max_wait", name)
			}
			v, quoted, err := p.parseValue()
			if err != nil {
				return nil, err
			}
			out.MaxWait = v
			out.MaxWaitQuoted = quoted
			out.MaxWaitSet = true
		case "tls":
			if out.TLS != nil {
				return nil, p.errAt(dirTok.pos, "duplicate %s tls block", name)
			}
			tls, err := p.parseTLSBlock()
			if err != nil {
				return nil, err
			}
			out.TLS = tls
		case "auth":
			typTok, err := p.expect(tokIdent, "expected auth type after %s.auth", name)
			if err != nil {
				return nil, err
			}
			switch typTok.text {
			case "token":
				v, quoted, err := p.parseValue()
				if err != nil {
					return nil, err
				}
				out.AuthTokens = append(out.AuthTokens, v)
				out.AuthTokensQuoted = append(out.AuthTokensQuoted, quoted)
			default:
				return nil, p.errAt(typTok.pos, "unknown auth type %q", typTok.text)
			}
		default:
			return nil, p.errAt(dirTok.pos, "unknown %s directive %q", name, dirTok.text)
		}
	}

	return out, nil
}

func (p *parser) parseTLSBlock() (*TLSBlock, error) {
	if _, err := p.expect(tokLBrace, "expected '{' after tls"); err != nil {
		return nil, err
	}
	out := &TLSBlock{}

	for {
		tok, err := p.peek()
		if err != nil {
			return nil, err
		}
		if tok.kind == tokEOF {
			return nil, p.errAt(tok.pos, "unexpected EOF (missing '}')")
		}
		if tok.kind == tokRBrace {
			_, _ = p.next()
			break
		}
		if tok.kind == tokComment {
			_, _ = p.next()
			continue
		}

		dirTok, _ := p.next()
		if dirTok.kind != tokIdent {
			return nil, p.errAt(dirTok.pos, "expected directive name")
		}
		switch dirTok.text {
		case "cert_file":
			if out.CertFileSet {
				return nil, p.errAt(dirTok.pos, "duplicate tls cert_file")
			}
			v, quoted, err := p.parseValue()
			if err != nil {
				return nil, err
			}
			out.CertFile = v
			out.CertFileQuoted = quoted
			out.CertFileSet = true
		case "key_file":
			if out.KeyFileSet {
				return nil, p.errAt(dirTok.pos, "duplicate tls key_file")
			}
			v, quoted, err := p.parseValue()
			if err != nil {
				return nil, err
			}
			out.KeyFile = v
			out.KeyFileQuoted = quoted
			out.KeyFileSet = true
		case "client_ca":
			if out.ClientCASet {
				return nil, p.errAt(dirTok.pos, "duplicate tls client_ca")
			}
			v, quoted, err := p.parseValue()
			if err != nil {
				return nil, err
			}
			out.ClientCA = v
			out.ClientCAQuoted = quoted
			out.ClientCASet = true
		case "client_auth":
			if out.ClientAuthSet {
				return nil, p.errAt(dirTok.pos, "duplicate tls client_auth")
			}
			v, quoted, err := p.parseValue()
			if err != nil {
				return nil, err
			}
			out.ClientAuth = v
			out.ClientAuthQuoted = quoted
			out.ClientAuthSet = true
		default:
			return nil, p.errAt(dirTok.pos, "unknown tls directive %q", dirTok.text)
		}
	}

	return out, nil
}

func (p *parser) parseObservabilityBlock() (*ObservabilityBlock, error) {
	if _, err := p.expect(tokLBrace, "expected '{' after observability"); err != nil {
		return nil, err
	}
	out := &ObservabilityBlock{}

	for {
		tok, err := p.peek()
		if err != nil {
			return nil, err
		}
		if tok.kind == tokEOF {
			return nil, p.errAt(tok.pos, "unexpected EOF (missing '}')")
		}
		if tok.kind == tokRBrace {
			_, _ = p.next()
			break
		}
		if tok.kind == tokComment {
			_, _ = p.next()
			continue
		}

		dirTok, _ := p.next()
		if dirTok.kind != tokIdent {
			return nil, p.errAt(dirTok.pos, "expected directive name")
		}
		switch dirTok.text {
		case "access_log":
			if out.AccessLogSet || out.AccessLogBlock != nil {
				return nil, p.errAt(dirTok.pos, "duplicate access_log directive")
			}
			nextTok, err := p.peek()
			if err != nil {
				return nil, err
			}
			if nextTok.kind == tokLBrace {
				b, err := p.parseAccessLogBlock()
				if err != nil {
					return nil, err
				}
				out.AccessLogBlock = b
				break
			}
			v, quoted, err := p.parseValue()
			if err != nil {
				return nil, err
			}
			out.AccessLog = v
			out.AccessLogQuoted = quoted
			out.AccessLogSet = true
		case "runtime_log":
			if out.RuntimeLogSet || out.RuntimeLogBlock != nil {
				return nil, p.errAt(dirTok.pos, "duplicate runtime_log directive")
			}
			nextTok, err := p.peek()
			if err != nil {
				return nil, err
			}
			if nextTok.kind == tokLBrace {
				b, err := p.parseRuntimeLogBlock()
				if err != nil {
					return nil, err
				}
				out.RuntimeLogBlock = b
				break
			}
			v, quoted, err := p.parseValue()
			if err != nil {
				return nil, err
			}
			out.RuntimeLog = v
			out.RuntimeLogQuoted = quoted
			out.RuntimeLogSet = true
		case "metrics":
			if out.Metrics != nil {
				return nil, p.errAt(dirTok.pos, "duplicate metrics directive")
			}
			nextTok, err := p.peek()
			if err != nil {
				return nil, err
			}
			if nextTok.kind == tokLBrace {
				m, err := p.parseMetricsBlock()
				if err != nil {
					return nil, err
				}
				out.Metrics = m
				break
			}
			v, quoted, err := p.parseValue()
			if err != nil {
				return nil, err
			}
			out.Metrics = &MetricsBlock{
				Enabled:       v,
				EnabledQuoted: quoted,
				EnabledSet:    true,
				shorthand:     true,
			}
		case "tracing":
			if out.Tracing != nil {
				return nil, p.errAt(dirTok.pos, "duplicate tracing directive")
			}
			nextTok, err := p.peek()
			if err != nil {
				return nil, err
			}
			if nextTok.kind == tokLBrace {
				t, err := p.parseTracingBlock()
				if err != nil {
					return nil, err
				}
				out.Tracing = t
				break
			}
			v, quoted, err := p.parseValue()
			if err != nil {
				return nil, err
			}
			out.Tracing = &TracingBlock{
				Enabled:       v,
				EnabledQuoted: quoted,
				EnabledSet:    true,
				shorthand:     true,
			}
		default:
			return nil, p.errAt(dirTok.pos, "unknown observability directive %q", dirTok.text)
		}
	}

	return out, nil
}

func (p *parser) parseAccessLogBlock() (*AccessLogBlock, error) {
	if _, err := p.expect(tokLBrace, "expected '{' after access_log"); err != nil {
		return nil, err
	}
	out := &AccessLogBlock{}

	for {
		tok, err := p.peek()
		if err != nil {
			return nil, err
		}
		if tok.kind == tokEOF {
			return nil, p.errAt(tok.pos, "unexpected EOF (missing '}')")
		}
		if tok.kind == tokRBrace {
			_, _ = p.next()
			break
		}
		if tok.kind == tokComment {
			_, _ = p.next()
			continue
		}

		dirTok, _ := p.next()
		if dirTok.kind != tokIdent {
			return nil, p.errAt(dirTok.pos, "expected directive name")
		}
		switch dirTok.text {
		case "enabled":
			if out.EnabledSet {
				return nil, p.errAt(dirTok.pos, "duplicate access_log enabled")
			}
			v, quoted, err := p.parseValue()
			if err != nil {
				return nil, err
			}
			out.Enabled = v
			out.EnabledQuoted = quoted
			out.EnabledSet = true
		case "output":
			if out.OutputSet {
				return nil, p.errAt(dirTok.pos, "duplicate access_log output")
			}
			v, quoted, err := p.parseValue()
			if err != nil {
				return nil, err
			}
			out.Output = v
			out.OutputQuoted = quoted
			out.OutputSet = true
		case "path":
			if out.PathSet {
				return nil, p.errAt(dirTok.pos, "duplicate access_log path")
			}
			v, quoted, err := p.parseValue()
			if err != nil {
				return nil, err
			}
			out.Path = v
			out.PathQuoted = quoted
			out.PathSet = true
		case "format":
			if out.FormatSet {
				return nil, p.errAt(dirTok.pos, "duplicate access_log format")
			}
			v, quoted, err := p.parseValue()
			if err != nil {
				return nil, err
			}
			out.Format = v
			out.FormatQuoted = quoted
			out.FormatSet = true
		default:
			return nil, p.errAt(dirTok.pos, "unknown access_log directive %q", dirTok.text)
		}
	}

	return out, nil
}

func (p *parser) parseRuntimeLogBlock() (*RuntimeLogBlock, error) {
	if _, err := p.expect(tokLBrace, "expected '{' after runtime_log"); err != nil {
		return nil, err
	}
	out := &RuntimeLogBlock{}

	for {
		tok, err := p.peek()
		if err != nil {
			return nil, err
		}
		if tok.kind == tokEOF {
			return nil, p.errAt(tok.pos, "unexpected EOF (missing '}')")
		}
		if tok.kind == tokRBrace {
			_, _ = p.next()
			break
		}
		if tok.kind == tokComment {
			_, _ = p.next()
			continue
		}

		dirTok, _ := p.next()
		if dirTok.kind != tokIdent {
			return nil, p.errAt(dirTok.pos, "expected directive name")
		}
		switch dirTok.text {
		case "level":
			if out.LevelSet {
				return nil, p.errAt(dirTok.pos, "duplicate runtime_log level")
			}
			v, quoted, err := p.parseValue()
			if err != nil {
				return nil, err
			}
			out.Level = v
			out.LevelQuoted = quoted
			out.LevelSet = true
		case "output":
			if out.OutputSet {
				return nil, p.errAt(dirTok.pos, "duplicate runtime_log output")
			}
			v, quoted, err := p.parseValue()
			if err != nil {
				return nil, err
			}
			out.Output = v
			out.OutputQuoted = quoted
			out.OutputSet = true
		case "path":
			if out.PathSet {
				return nil, p.errAt(dirTok.pos, "duplicate runtime_log path")
			}
			v, quoted, err := p.parseValue()
			if err != nil {
				return nil, err
			}
			out.Path = v
			out.PathQuoted = quoted
			out.PathSet = true
		case "format":
			if out.FormatSet {
				return nil, p.errAt(dirTok.pos, "duplicate runtime_log format")
			}
			v, quoted, err := p.parseValue()
			if err != nil {
				return nil, err
			}
			out.Format = v
			out.FormatQuoted = quoted
			out.FormatSet = true
		default:
			return nil, p.errAt(dirTok.pos, "unknown runtime_log directive %q", dirTok.text)
		}
	}

	return out, nil
}

func isEgressDirective(name string) bool {
	switch strings.TrimSpace(name) {
	case "allow", "deny", "https_only", "redirects", "dns_rebind_protection":
		return true
	default:
		return false
	}
}

func (p *parser) parseEgressBlock() (*EgressBlock, error) {
	if _, err := p.expect(tokLBrace, "expected '{' after egress"); err != nil {
		return nil, err
	}
	out := &EgressBlock{}

	for {
		tok, err := p.peek()
		if err != nil {
			return nil, err
		}
		if tok.kind == tokEOF {
			return nil, p.errAt(tok.pos, "unexpected EOF (missing '}')")
		}
		if tok.kind == tokRBrace {
			_, _ = p.next()
			break
		}
		if tok.kind == tokComment {
			_, _ = p.next()
			continue
		}

		dirTok, _ := p.next()
		if dirTok.kind != tokIdent {
			return nil, p.errAt(dirTok.pos, "expected directive name")
		}
		switch dirTok.text {
		case "allow":
			for {
				v, quoted, err := p.parseValue()
				if err != nil {
					return nil, err
				}
				out.Allow = append(out.Allow, v)
				out.AllowQuoted = append(out.AllowQuoted, quoted)

				next, err := p.peek()
				if err != nil {
					return nil, err
				}
				if next.kind == tokEOF || next.kind == tokRBrace || next.kind == tokComment {
					break
				}
				if next.kind == tokIdent && isEgressDirective(next.text) {
					break
				}
			}
		case "deny":
			for {
				v, quoted, err := p.parseValue()
				if err != nil {
					return nil, err
				}
				out.Deny = append(out.Deny, v)
				out.DenyQuoted = append(out.DenyQuoted, quoted)

				next, err := p.peek()
				if err != nil {
					return nil, err
				}
				if next.kind == tokEOF || next.kind == tokRBrace || next.kind == tokComment {
					break
				}
				if next.kind == tokIdent && isEgressDirective(next.text) {
					break
				}
			}
		case "https_only":
			if out.HTTPSOnlySet {
				return nil, p.errAt(dirTok.pos, "duplicate egress https_only")
			}
			v, quoted, err := p.parseValue()
			if err != nil {
				return nil, err
			}
			out.HTTPSOnly = v
			out.HTTPSOnlyQuoted = quoted
			out.HTTPSOnlySet = true
		case "redirects":
			if out.RedirectsSet {
				return nil, p.errAt(dirTok.pos, "duplicate egress redirects")
			}
			v, quoted, err := p.parseValue()
			if err != nil {
				return nil, err
			}
			out.Redirects = v
			out.RedirectsQuoted = quoted
			out.RedirectsSet = true
		case "dns_rebind_protection":
			if out.DNSRebindProtectionSet {
				return nil, p.errAt(dirTok.pos, "duplicate egress dns_rebind_protection")
			}
			v, quoted, err := p.parseValue()
			if err != nil {
				return nil, err
			}
			out.DNSRebindProtection = v
			out.DNSRebindProtectionQuoted = quoted
			out.DNSRebindProtectionSet = true
		default:
			return nil, p.errAt(dirTok.pos, "unknown egress directive %q", dirTok.text)
		}
	}

	return out, nil
}

func (p *parser) parsePublishPolicyBlock() (*PublishPolicyBlock, error) {
	if _, err := p.expect(tokLBrace, "expected '{' after publish_policy"); err != nil {
		return nil, err
	}
	out := &PublishPolicyBlock{}

	for {
		tok, err := p.peek()
		if err != nil {
			return nil, err
		}
		if tok.kind == tokEOF {
			return nil, p.errAt(tok.pos, "unexpected EOF (missing '}')")
		}
		if tok.kind == tokRBrace {
			_, _ = p.next()
			break
		}
		if tok.kind == tokComment {
			_, _ = p.next()
			continue
		}

		dirTok, _ := p.next()
		if dirTok.kind != tokIdent {
			return nil, p.errAt(dirTok.pos, "expected directive name")
		}
		switch dirTok.text {
		case "direct":
			if out.DirectSet {
				return nil, p.errAt(dirTok.pos, "duplicate publish_policy direct")
			}
			v, quoted, err := p.parseValue()
			if err != nil {
				return nil, err
			}
			out.Direct = v
			out.DirectQuoted = quoted
			out.DirectSet = true
		case "managed":
			if out.ManagedSet {
				return nil, p.errAt(dirTok.pos, "duplicate publish_policy managed")
			}
			v, quoted, err := p.parseValue()
			if err != nil {
				return nil, err
			}
			out.Managed = v
			out.ManagedQuoted = quoted
			out.ManagedSet = true
		case "allow_pull_routes":
			if out.AllowPullRoutesSet {
				return nil, p.errAt(dirTok.pos, "duplicate publish_policy allow_pull_routes")
			}
			v, quoted, err := p.parseValue()
			if err != nil {
				return nil, err
			}
			out.AllowPullRoutes = v
			out.AllowPullRoutesQuoted = quoted
			out.AllowPullRoutesSet = true
		case "allow_deliver_routes":
			if out.AllowDeliverRoutesSet {
				return nil, p.errAt(dirTok.pos, "duplicate publish_policy allow_deliver_routes")
			}
			v, quoted, err := p.parseValue()
			if err != nil {
				return nil, err
			}
			out.AllowDeliverRoutes = v
			out.AllowDeliverRoutesQuoted = quoted
			out.AllowDeliverRoutesSet = true
		case "require_actor":
			if out.RequireActorSet {
				return nil, p.errAt(dirTok.pos, "duplicate publish_policy require_actor")
			}
			v, quoted, err := p.parseValue()
			if err != nil {
				return nil, err
			}
			out.RequireActor = v
			out.RequireActorQuoted = quoted
			out.RequireActorSet = true
		case "require_request_id":
			if out.RequireRequestIDSet {
				return nil, p.errAt(dirTok.pos, "duplicate publish_policy require_request_id")
			}
			v, quoted, err := p.parseValue()
			if err != nil {
				return nil, err
			}
			out.RequireRequestID = v
			out.RequireRequestIDQuoted = quoted
			out.RequireRequestIDSet = true
		case "fail_closed":
			if out.FailClosedSet {
				return nil, p.errAt(dirTok.pos, "duplicate publish_policy fail_closed")
			}
			v, quoted, err := p.parseValue()
			if err != nil {
				return nil, err
			}
			out.FailClosed = v
			out.FailClosedQuoted = quoted
			out.FailClosedSet = true
		case "actor_allow":
			for {
				v, quoted, err := p.parseValue()
				if err != nil {
					return nil, err
				}
				out.ActorAllow = append(out.ActorAllow, v)
				out.ActorAllowQuoted = append(out.ActorAllowQuoted, quoted)

				next, err := p.peek()
				if err != nil {
					return nil, err
				}
				if next.kind == tokEOF || next.kind == tokRBrace || next.kind == tokComment {
					break
				}
				if next.kind == tokIdent && isPublishPolicyDirective(next.text) {
					break
				}
			}
		case "actor_prefix":
			for {
				v, quoted, err := p.parseValue()
				if err != nil {
					return nil, err
				}
				out.ActorPrefix = append(out.ActorPrefix, v)
				out.ActorPrefixQuoted = append(out.ActorPrefixQuoted, quoted)

				next, err := p.peek()
				if err != nil {
					return nil, err
				}
				if next.kind == tokEOF || next.kind == tokRBrace || next.kind == tokComment {
					break
				}
				if next.kind == tokIdent && isPublishPolicyDirective(next.text) {
					break
				}
			}
		default:
			return nil, p.errAt(dirTok.pos, "unknown publish_policy directive %q", dirTok.text)
		}
	}

	return out, nil
}

func isPublishPolicyDirective(name string) bool {
	switch strings.TrimSpace(name) {
	case "direct",
		"managed",
		"allow_pull_routes",
		"allow_deliver_routes",
		"require_actor",
		"require_request_id",
		"fail_closed",
		"actor_allow",
		"actor_prefix":
		return true
	default:
		return false
	}
}

func (p *parser) parseMetricsBlock() (*MetricsBlock, error) {
	if _, err := p.expect(tokLBrace, "expected '{' after metrics"); err != nil {
		return nil, err
	}
	out := &MetricsBlock{}

	for {
		tok, err := p.peek()
		if err != nil {
			return nil, err
		}
		if tok.kind == tokEOF {
			return nil, p.errAt(tok.pos, "unexpected EOF (missing '}')")
		}
		if tok.kind == tokRBrace {
			_, _ = p.next()
			break
		}
		if tok.kind == tokComment {
			_, _ = p.next()
			continue
		}

		dirTok, _ := p.next()
		if dirTok.kind != tokIdent {
			return nil, p.errAt(dirTok.pos, "expected directive name")
		}
		switch dirTok.text {
		case "enabled":
			if out.EnabledSet {
				return nil, p.errAt(dirTok.pos, "duplicate metrics enabled")
			}
			v, quoted, err := p.parseValue()
			if err != nil {
				return nil, err
			}
			out.Enabled = v
			out.EnabledQuoted = quoted
			out.EnabledSet = true
		case "listen":
			if out.ListenSet {
				return nil, p.errAt(dirTok.pos, "duplicate metrics listen")
			}
			v, quoted, err := p.parseValue()
			if err != nil {
				return nil, err
			}
			out.Listen = v
			out.ListenQuoted = quoted
			out.ListenSet = true
		case "prefix":
			if out.PrefixSet {
				return nil, p.errAt(dirTok.pos, "duplicate metrics prefix")
			}
			v, quoted, err := p.parseValue()
			if err != nil {
				return nil, err
			}
			out.Prefix = v
			out.PrefixQuoted = quoted
			out.PrefixSet = true
		default:
			return nil, p.errAt(dirTok.pos, "unknown metrics directive %q", dirTok.text)
		}
	}

	return out, nil
}

func (p *parser) parseTracingBlock() (*TracingBlock, error) {
	if _, err := p.expect(tokLBrace, "expected '{' after tracing"); err != nil {
		return nil, err
	}
	out := &TracingBlock{}

	for {
		tok, err := p.peek()
		if err != nil {
			return nil, err
		}
		if tok.kind == tokEOF {
			return nil, p.errAt(tok.pos, "unexpected EOF (missing '}')")
		}
		if tok.kind == tokRBrace {
			_, _ = p.next()
			break
		}
		if tok.kind == tokComment {
			_, _ = p.next()
			continue
		}

		dirTok, _ := p.next()
		if dirTok.kind != tokIdent {
			return nil, p.errAt(dirTok.pos, "expected directive name")
		}
		switch dirTok.text {
		case "enabled":
			if out.EnabledSet {
				return nil, p.errAt(dirTok.pos, "duplicate tracing enabled")
			}
			v, quoted, err := p.parseValue()
			if err != nil {
				return nil, err
			}
			out.Enabled = v
			out.EnabledQuoted = quoted
			out.EnabledSet = true
		case "collector":
			if out.CollectorSet {
				return nil, p.errAt(dirTok.pos, "duplicate tracing collector")
			}
			v, quoted, err := p.parseValue()
			if err != nil {
				return nil, err
			}
			out.Collector = v
			out.CollectorQuoted = quoted
			out.CollectorSet = true
		case "url_path":
			if out.URLPathSet {
				return nil, p.errAt(dirTok.pos, "duplicate tracing url_path")
			}
			v, quoted, err := p.parseValue()
			if err != nil {
				return nil, err
			}
			out.URLPath = v
			out.URLPathQuoted = quoted
			out.URLPathSet = true
		case "timeout":
			if out.TimeoutSet {
				return nil, p.errAt(dirTok.pos, "duplicate tracing timeout")
			}
			v, quoted, err := p.parseValue()
			if err != nil {
				return nil, err
			}
			out.Timeout = v
			out.TimeoutQuoted = quoted
			out.TimeoutSet = true
		case "compression":
			if out.CompressionSet {
				return nil, p.errAt(dirTok.pos, "duplicate tracing compression")
			}
			v, quoted, err := p.parseValue()
			if err != nil {
				return nil, err
			}
			out.Compression = v
			out.CompressionQuoted = quoted
			out.CompressionSet = true
		case "insecure":
			if out.InsecureSet {
				return nil, p.errAt(dirTok.pos, "duplicate tracing insecure")
			}
			v, quoted, err := p.parseValue()
			if err != nil {
				return nil, err
			}
			out.Insecure = v
			out.InsecureQuoted = quoted
			out.InsecureSet = true
		case "proxy_url":
			if out.ProxyURLSet {
				return nil, p.errAt(dirTok.pos, "duplicate tracing proxy_url")
			}
			v, quoted, err := p.parseValue()
			if err != nil {
				return nil, err
			}
			out.ProxyURL = v
			out.ProxyURLQuoted = quoted
			out.ProxyURLSet = true
		case "tls":
			if out.TLS != nil {
				return nil, p.errAt(dirTok.pos, "duplicate tracing tls block")
			}
			t, err := p.parseTracingTLSBlock()
			if err != nil {
				return nil, err
			}
			out.TLS = t
		case "retry":
			if out.Retry != nil {
				return nil, p.errAt(dirTok.pos, "duplicate tracing retry block")
			}
			r, err := p.parseTracingRetryBlock()
			if err != nil {
				return nil, err
			}
			out.Retry = r
		case "header":
			name, nameQuoted, err := p.parseValue()
			if err != nil {
				return nil, err
			}
			value, valueQuoted, err := p.parseValue()
			if err != nil {
				return nil, err
			}
			out.Headers = append(out.Headers, TracingHeader{
				Name:        name,
				NameQuoted:  nameQuoted,
				Value:       value,
				ValueQuoted: valueQuoted,
			})
		default:
			return nil, p.errAt(dirTok.pos, "unknown tracing directive %q", dirTok.text)
		}
	}

	return out, nil
}

func (p *parser) parseTracingTLSBlock() (*TracingTLSBlock, error) {
	if _, err := p.expect(tokLBrace, "expected '{' after tls"); err != nil {
		return nil, err
	}
	out := &TracingTLSBlock{}

	for {
		tok, err := p.peek()
		if err != nil {
			return nil, err
		}
		if tok.kind == tokEOF {
			return nil, p.errAt(tok.pos, "unexpected EOF (missing '}')")
		}
		if tok.kind == tokRBrace {
			_, _ = p.next()
			break
		}
		if tok.kind == tokComment {
			_, _ = p.next()
			continue
		}

		dirTok, _ := p.next()
		if dirTok.kind != tokIdent {
			return nil, p.errAt(dirTok.pos, "expected directive name")
		}
		switch dirTok.text {
		case "ca_file":
			if out.CAFileSet {
				return nil, p.errAt(dirTok.pos, "duplicate tls ca_file")
			}
			v, quoted, err := p.parseValue()
			if err != nil {
				return nil, err
			}
			out.CAFile = v
			out.CAFileQuoted = quoted
			out.CAFileSet = true
		case "cert_file":
			if out.CertFileSet {
				return nil, p.errAt(dirTok.pos, "duplicate tls cert_file")
			}
			v, quoted, err := p.parseValue()
			if err != nil {
				return nil, err
			}
			out.CertFile = v
			out.CertFileQuoted = quoted
			out.CertFileSet = true
		case "key_file":
			if out.KeyFileSet {
				return nil, p.errAt(dirTok.pos, "duplicate tls key_file")
			}
			v, quoted, err := p.parseValue()
			if err != nil {
				return nil, err
			}
			out.KeyFile = v
			out.KeyFileQuoted = quoted
			out.KeyFileSet = true
		case "server_name":
			if out.ServerNameSet {
				return nil, p.errAt(dirTok.pos, "duplicate tls server_name")
			}
			v, quoted, err := p.parseValue()
			if err != nil {
				return nil, err
			}
			out.ServerName = v
			out.ServerNameQuoted = quoted
			out.ServerNameSet = true
		case "insecure_skip_verify":
			if out.InsecureSkipVerifySet {
				return nil, p.errAt(dirTok.pos, "duplicate tls insecure_skip_verify")
			}
			v, quoted, err := p.parseValue()
			if err != nil {
				return nil, err
			}
			out.InsecureSkipVerify = v
			out.InsecureSkipVerifyQuoted = quoted
			out.InsecureSkipVerifySet = true
		default:
			return nil, p.errAt(dirTok.pos, "unknown tls directive %q", dirTok.text)
		}
	}

	return out, nil
}

func (p *parser) parseTracingRetryBlock() (*TracingRetryBlock, error) {
	if _, err := p.expect(tokLBrace, "expected '{' after retry"); err != nil {
		return nil, err
	}
	out := &TracingRetryBlock{}

	for {
		tok, err := p.peek()
		if err != nil {
			return nil, err
		}
		if tok.kind == tokEOF {
			return nil, p.errAt(tok.pos, "unexpected EOF (missing '}')")
		}
		if tok.kind == tokRBrace {
			_, _ = p.next()
			break
		}
		if tok.kind == tokComment {
			_, _ = p.next()
			continue
		}

		dirTok, _ := p.next()
		if dirTok.kind != tokIdent {
			return nil, p.errAt(dirTok.pos, "expected directive name")
		}
		switch dirTok.text {
		case "enabled":
			if out.EnabledSet {
				return nil, p.errAt(dirTok.pos, "duplicate tracing retry enabled")
			}
			v, quoted, err := p.parseValue()
			if err != nil {
				return nil, err
			}
			out.Enabled = v
			out.EnabledQuoted = quoted
			out.EnabledSet = true
		case "initial_interval":
			if out.InitialIntervalSet {
				return nil, p.errAt(dirTok.pos, "duplicate tracing retry initial_interval")
			}
			v, quoted, err := p.parseValue()
			if err != nil {
				return nil, err
			}
			out.InitialInterval = v
			out.InitialIntervalQuoted = quoted
			out.InitialIntervalSet = true
		case "max_interval":
			if out.MaxIntervalSet {
				return nil, p.errAt(dirTok.pos, "duplicate tracing retry max_interval")
			}
			v, quoted, err := p.parseValue()
			if err != nil {
				return nil, err
			}
			out.MaxInterval = v
			out.MaxIntervalQuoted = quoted
			out.MaxIntervalSet = true
		case "max_elapsed_time":
			if out.MaxElapsedTimeSet {
				return nil, p.errAt(dirTok.pos, "duplicate tracing retry max_elapsed_time")
			}
			v, quoted, err := p.parseValue()
			if err != nil {
				return nil, err
			}
			out.MaxElapsedTime = v
			out.MaxElapsedTimeQuoted = quoted
			out.MaxElapsedTimeSet = true
		default:
			return nil, p.errAt(dirTok.pos, "unknown tracing retry directive %q", dirTok.text)
		}
	}

	return out, nil
}

func (p *parser) parseQueueLimitsBlock() (*QueueLimitsBlock, error) {
	if _, err := p.expect(tokLBrace, "expected '{' after queue_limits"); err != nil {
		return nil, err
	}
	out := &QueueLimitsBlock{}

	for {
		tok, err := p.peek()
		if err != nil {
			return nil, err
		}
		if tok.kind == tokEOF {
			return nil, p.errAt(tok.pos, "unexpected EOF (missing '}')")
		}
		if tok.kind == tokRBrace {
			_, _ = p.next()
			break
		}
		if tok.kind == tokComment {
			_, _ = p.next()
			continue
		}

		dirTok, _ := p.next()
		if dirTok.kind != tokIdent {
			return nil, p.errAt(dirTok.pos, "expected directive name")
		}
		switch dirTok.text {
		case "max_depth":
			if out.MaxDepthSet {
				return nil, p.errAt(dirTok.pos, "duplicate queue_limits max_depth")
			}
			v, quoted, err := p.parseValue()
			if err != nil {
				return nil, err
			}
			out.MaxDepth = v
			out.MaxDepthQuoted = quoted
			out.MaxDepthSet = true
		case "drop_policy":
			if out.DropPolicySet {
				return nil, p.errAt(dirTok.pos, "duplicate queue_limits drop_policy")
			}
			v, quoted, err := p.parseValue()
			if err != nil {
				return nil, err
			}
			out.DropPolicy = v
			out.DropPolicyQuoted = quoted
			out.DropPolicySet = true
		default:
			return nil, p.errAt(dirTok.pos, "unknown queue_limits directive %q", dirTok.text)
		}
	}

	return out, nil
}

func (p *parser) parseQueueRetentionBlock() (*QueueRetentionBlock, error) {
	if _, err := p.expect(tokLBrace, "expected '{' after queue_retention"); err != nil {
		return nil, err
	}
	out := &QueueRetentionBlock{}

	for {
		tok, err := p.peek()
		if err != nil {
			return nil, err
		}
		if tok.kind == tokEOF {
			return nil, p.errAt(tok.pos, "unexpected EOF (missing '}')")
		}
		if tok.kind == tokRBrace {
			_, _ = p.next()
			break
		}
		if tok.kind == tokComment {
			_, _ = p.next()
			continue
		}

		dirTok, _ := p.next()
		if dirTok.kind != tokIdent {
			return nil, p.errAt(dirTok.pos, "expected directive name")
		}
		switch dirTok.text {
		case "max_age":
			if out.MaxAgeSet {
				return nil, p.errAt(dirTok.pos, "duplicate queue_retention max_age")
			}
			v, quoted, err := p.parseValue()
			if err != nil {
				return nil, err
			}
			out.MaxAge = v
			out.MaxAgeQuoted = quoted
			out.MaxAgeSet = true
		case "prune_interval":
			if out.PruneIntervalSet {
				return nil, p.errAt(dirTok.pos, "duplicate queue_retention prune_interval")
			}
			v, quoted, err := p.parseValue()
			if err != nil {
				return nil, err
			}
			out.PruneInterval = v
			out.PruneIntervalQuoted = quoted
			out.PruneIntervalSet = true
		default:
			return nil, p.errAt(dirTok.pos, "unknown queue_retention directive %q", dirTok.text)
		}
	}

	return out, nil
}

func (p *parser) parseDeliveredRetentionBlock() (*DeliveredRetentionBlock, error) {
	if _, err := p.expect(tokLBrace, "expected '{' after delivered_retention"); err != nil {
		return nil, err
	}
	out := &DeliveredRetentionBlock{}

	for {
		tok, err := p.peek()
		if err != nil {
			return nil, err
		}
		if tok.kind == tokEOF {
			return nil, p.errAt(tok.pos, "unexpected EOF (missing '}')")
		}
		if tok.kind == tokRBrace {
			_, _ = p.next()
			break
		}
		if tok.kind == tokComment {
			_, _ = p.next()
			continue
		}

		dirTok, _ := p.next()
		if dirTok.kind != tokIdent {
			return nil, p.errAt(dirTok.pos, "expected directive name")
		}
		switch dirTok.text {
		case "max_age":
			if out.MaxAgeSet {
				return nil, p.errAt(dirTok.pos, "duplicate delivered_retention max_age")
			}
			v, quoted, err := p.parseValue()
			if err != nil {
				return nil, err
			}
			out.MaxAge = v
			out.MaxAgeQuoted = quoted
			out.MaxAgeSet = true
		default:
			return nil, p.errAt(dirTok.pos, "unknown delivered_retention directive %q", dirTok.text)
		}
	}

	return out, nil
}

func (p *parser) parseDLQRetentionBlock() (*DLQRetentionBlock, error) {
	if _, err := p.expect(tokLBrace, "expected '{' after dlq_retention"); err != nil {
		return nil, err
	}
	out := &DLQRetentionBlock{}

	for {
		tok, err := p.peek()
		if err != nil {
			return nil, err
		}
		if tok.kind == tokEOF {
			return nil, p.errAt(tok.pos, "unexpected EOF (missing '}')")
		}
		if tok.kind == tokRBrace {
			_, _ = p.next()
			break
		}
		if tok.kind == tokComment {
			_, _ = p.next()
			continue
		}

		dirTok, _ := p.next()
		if dirTok.kind != tokIdent {
			return nil, p.errAt(dirTok.pos, "expected directive name")
		}
		switch dirTok.text {
		case "max_age":
			if out.MaxAgeSet {
				return nil, p.errAt(dirTok.pos, "duplicate dlq_retention max_age")
			}
			v, quoted, err := p.parseValue()
			if err != nil {
				return nil, err
			}
			out.MaxAge = v
			out.MaxAgeQuoted = quoted
			out.MaxAgeSet = true
		case "max_depth":
			if out.MaxDepthSet {
				return nil, p.errAt(dirTok.pos, "duplicate dlq_retention max_depth")
			}
			v, quoted, err := p.parseValue()
			if err != nil {
				return nil, err
			}
			out.MaxDepth = v
			out.MaxDepthQuoted = quoted
			out.MaxDepthSet = true
		default:
			return nil, p.errAt(dirTok.pos, "unknown dlq_retention directive %q", dirTok.text)
		}
	}

	return out, nil
}

func (p *parser) parseRateLimitBlock() (*RateLimitBlock, error) {
	if _, err := p.expect(tokLBrace, "expected '{' after rate_limit"); err != nil {
		return nil, err
	}
	out := &RateLimitBlock{}

	for {
		tok, err := p.peek()
		if err != nil {
			return nil, err
		}
		if tok.kind == tokEOF {
			return nil, p.errAt(tok.pos, "unexpected EOF (missing '}')")
		}
		if tok.kind == tokRBrace {
			_, _ = p.next()
			break
		}
		if tok.kind == tokComment {
			_, _ = p.next()
			continue
		}

		dirTok, _ := p.next()
		if dirTok.kind != tokIdent {
			return nil, p.errAt(dirTok.pos, "expected directive name")
		}
		switch dirTok.text {
		case "rps":
			if out.RPSSet {
				return nil, p.errAt(dirTok.pos, "duplicate rate_limit rps")
			}
			v, quoted, err := p.parseValue()
			if err != nil {
				return nil, err
			}
			out.RPS = v
			out.RPSQuoted = quoted
			out.RPSSet = true
		case "burst":
			if out.BurstSet {
				return nil, p.errAt(dirTok.pos, "duplicate rate_limit burst")
			}
			v, quoted, err := p.parseValue()
			if err != nil {
				return nil, err
			}
			out.Burst = v
			out.BurstQuoted = quoted
			out.BurstSet = true
		default:
			return nil, p.errAt(dirTok.pos, "unknown rate_limit directive %q", dirTok.text)
		}
	}

	return out, nil
}

func (p *parser) parseRoutePublishBlock() (*PublishBlock, error) {
	// Check if this is a shorthand: `publish on|off`
	next, err := p.peek()
	if err != nil {
		return nil, err
	}
	if next.kind == tokIdent || next.kind == tokString {
		// Could be shorthand value or '{'
		if next.text != "{" {
			v, quoted, err := p.parseValue()
			if err != nil {
				return nil, err
			}
			return &PublishBlock{
				Enabled:       v,
				EnabledQuoted: quoted,
				EnabledSet:    true,
				shorthand:     true,
			}, nil
		}
	}

	if _, err := p.expect(tokLBrace, "expected '{' after publish"); err != nil {
		return nil, err
	}
	out := &PublishBlock{}

	for {
		tok, err := p.peek()
		if err != nil {
			return nil, err
		}
		if tok.kind == tokEOF {
			return nil, p.errAt(tok.pos, "unexpected EOF (missing '}')")
		}
		if tok.kind == tokRBrace {
			_, _ = p.next()
			break
		}
		if tok.kind == tokComment {
			_, _ = p.next()
			continue
		}

		dirTok, _ := p.next()
		if dirTok.kind != tokIdent {
			return nil, p.errAt(dirTok.pos, "expected directive name")
		}
		switch dirTok.text {
		case "enabled":
			if out.EnabledSet {
				return nil, p.errAt(dirTok.pos, "duplicate publish enabled")
			}
			v, quoted, err := p.parseValue()
			if err != nil {
				return nil, err
			}
			out.Enabled = v
			out.EnabledQuoted = quoted
			out.EnabledSet = true
		case "direct":
			if out.DirectSet {
				return nil, p.errAt(dirTok.pos, "duplicate publish direct")
			}
			v, quoted, err := p.parseValue()
			if err != nil {
				return nil, err
			}
			out.Direct = v
			out.DirectQuoted = quoted
			out.DirectSet = true
		case "managed":
			if out.ManagedSet {
				return nil, p.errAt(dirTok.pos, "duplicate publish managed")
			}
			v, quoted, err := p.parseValue()
			if err != nil {
				return nil, err
			}
			out.Managed = v
			out.ManagedQuoted = quoted
			out.ManagedSet = true
		default:
			return nil, p.errAt(dirTok.pos, "unknown publish directive %q", dirTok.text)
		}
	}

	return out, nil
}

func (p *parser) parseRouteBlock() (Route, error) {
	pathTok, _ := p.next()
	var path string
	pathQuoted := false
	switch pathTok.kind {
	case tokString:
		path = pathTok.text
		pathQuoted = true
	case tokIdent:
		if !strings.HasPrefix(pathTok.text, "/") {
			return Route{}, p.errAt(pathTok.pos, "expected route path starting with '/'")
		}
		path = pathTok.text
	default:
		return Route{}, p.errAt(pathTok.pos, "expected route path")
	}
	if _, err := p.expect(tokLBrace, "expected '{' after route %q", path); err != nil {
		return Route{}, err
	}

	route := Route{Path: path, PathQuoted: pathQuoted}

	for {
		tok, err := p.peek()
		if err != nil {
			return Route{}, err
		}
		if tok.kind == tokEOF {
			return Route{}, p.errAt(tok.pos, "unexpected EOF (missing '}')")
		}
		if tok.kind == tokRBrace {
			_, _ = p.next()
			break
		}
		if tok.kind == tokComment {
			_, _ = p.next()
			continue
		}

		dirTok, _ := p.next()
		if dirTok.kind != tokIdent {
			return Route{}, p.errAt(dirTok.pos, "expected directive name")
		}
		switch dirTok.text {
		case "application":
			if route.ApplicationSet {
				return Route{}, p.errAt(dirTok.pos, "duplicate route application")
			}
			v, quoted, err := p.parseValue()
			if err != nil {
				return Route{}, err
			}
			route.Application = v
			route.ApplicationQuoted = quoted
			route.ApplicationSet = true
		case "endpoint_name":
			if route.EndpointNameSet {
				return Route{}, p.errAt(dirTok.pos, "duplicate route endpoint_name")
			}
			v, quoted, err := p.parseValue()
			if err != nil {
				return Route{}, err
			}
			route.EndpointName = v
			route.EndpointNameQuoted = quoted
			route.EndpointNameSet = true
		case "match":
			next, err := p.peek()
			if err != nil {
				return Route{}, err
			}
			if next.kind == tokIdent && strings.HasPrefix(next.text, "@") {
				for {
					refTok, _ := p.next()
					ref := strings.TrimPrefix(refTok.text, "@")
					if ref == "" {
						return Route{}, p.errAt(refTok.pos, "match reference must not be empty")
					}
					route.MatchRefs = append(route.MatchRefs, ref)

					next, err := p.peek()
					if err != nil {
						return Route{}, err
					}
					if next.kind != tokIdent || !strings.HasPrefix(next.text, "@") {
						break
					}
				}
				break
			}
			if route.Match != nil {
				return Route{}, p.errAt(dirTok.pos, "duplicate match block")
			}
			m, err := p.parseMatchBlock()
			if err != nil {
				return Route{}, err
			}
			route.Match = m
		case "rate_limit":
			if route.RateLimit != nil {
				return Route{}, p.errAt(dirTok.pos, "duplicate route rate_limit block")
			}
			rl, err := p.parseRateLimitBlock()
			if err != nil {
				return Route{}, err
			}
			route.RateLimit = rl
		case "queue":
			if route.Queue != nil {
				return Route{}, p.errAt(dirTok.pos, "duplicate queue block")
			}
			nextTok, err := p.peek()
			if err != nil {
				return Route{}, err
			}
			if nextTok.kind == tokLBrace {
				q, err := p.parseQueueBlock()
				if err != nil {
					return Route{}, err
				}
				route.Queue = q
				break
			}
			v, quoted, err := p.parseValue()
			if err != nil {
				return Route{}, err
			}
			route.Queue = &QueueBlock{
				Backend:       v,
				BackendQuoted: quoted,
				BackendSet:    true,
				shorthand:     true,
			}
		case "auth":
			typTok, err := p.expect(tokIdent, "expected auth type after auth")
			if err != nil {
				return Route{}, err
			}
			switch typTok.text {
			case "basic":
				user, userQuoted, err := p.parseValue()
				if err != nil {
					return Route{}, err
				}
				pass, passQuoted, err := p.parseValue()
				if err != nil {
					return Route{}, err
				}
				route.AuthBasic = append(route.AuthBasic, BasicAuth{
					User:       user,
					UserQuoted: userQuoted,
					Pass:       pass,
					PassQuoted: passQuoted,
				})
			case "hmac":
				next, err := p.peek()
				if err != nil {
					return Route{}, err
				}
				if next.kind == tokLBrace {
					if route.AuthHMACBlockSet {
						return Route{}, p.errAt(next.pos, "duplicate route auth hmac block")
					}
					if err := p.parseRouteAuthHMACBlock(&route); err != nil {
						return Route{}, err
					}
					route.AuthHMACBlockSet = true
					break
				}

				isRef := false
				if next.kind == tokIdent && next.text == "secret_ref" {
					_, _ = p.next()
					isRef = true
				}
				secret, quoted, err := p.parseValue()
				if err != nil {
					return Route{}, err
				}
				route.AuthHMACSecrets = append(route.AuthHMACSecrets, secret)
				route.AuthHMACSecretsQuoted = append(route.AuthHMACSecretsQuoted, quoted)
				route.AuthHMACSecretIsRef = append(route.AuthHMACSecretIsRef, isRef)

				next, err = p.peek()
				if err != nil {
					return Route{}, err
				}
				if next.kind == tokLBrace {
					if route.AuthHMACBlockSet {
						return Route{}, p.errAt(next.pos, "duplicate route auth hmac block")
					}
					if err := p.parseRouteAuthHMACBlock(&route); err != nil {
						return Route{}, err
					}
					route.AuthHMACBlockSet = true
				}
			case "forward":
				if route.AuthForward != nil {
					return Route{}, p.errAt(typTok.pos, "duplicate route auth forward")
				}
				v, quoted, err := p.parseValue()
				if err != nil {
					return Route{}, err
				}
				fwd := &ForwardAuthBlock{
					URL:       v,
					URLQuoted: quoted,
				}
				next, err := p.peek()
				if err != nil {
					return Route{}, err
				}
				if next.kind == tokLBrace {
					if err := p.parseRouteAuthForwardBlock(fwd); err != nil {
						return Route{}, err
					}
				}
				route.AuthForward = fwd
			default:
				return Route{}, p.errAt(typTok.pos, "unknown route auth type %q", typTok.text)
			}
		case "pull":
			if route.Pull != nil {
				return Route{}, p.errAt(dirTok.pos, "duplicate pull block")
			}
			pull, err := p.parsePullBlock()
			if err != nil {
				return Route{}, err
			}
			route.Pull = pull
		case "deliver":
			url, quoted, err := p.parseValue()
			if err != nil {
				return Route{}, err
			}
			deliver, err := p.parseDeliverBlock()
			if err != nil {
				return Route{}, err
			}
			deliver.URL = url
			deliver.URLQuoted = quoted
			route.Deliveries = append(route.Deliveries, *deliver)
		case "max_body":
			if route.MaxBodySet {
				return Route{}, p.errAt(dirTok.pos, "duplicate route max_body")
			}
			v, quoted, err := p.parseValue()
			if err != nil {
				return Route{}, err
			}
			route.MaxBody = v
			route.MaxBodyQuoted = quoted
			route.MaxBodySet = true
		case "max_headers":
			if route.MaxHeadersSet {
				return Route{}, p.errAt(dirTok.pos, "duplicate route max_headers")
			}
			v, quoted, err := p.parseValue()
			if err != nil {
				return Route{}, err
			}
			route.MaxHeaders = v
			route.MaxHeadersQuoted = quoted
			route.MaxHeadersSet = true
		case "publish":
			if route.Publish != nil {
				// If Publish was created by publish.direct/publish.managed, block form conflicts.
				if route.Publish.DirectSet || route.Publish.ManagedSet {
					return Route{}, p.errAt(dirTok.pos, "publish block conflicts with publish.direct/publish.managed")
				}
				return Route{}, p.errAt(dirTok.pos, "duplicate route publish")
			}
			pb, err := p.parseRoutePublishBlock()
			if err != nil {
				return Route{}, err
			}
			route.Publish = pb
		case "publish.direct":
			if route.Publish == nil {
				route.Publish = &PublishBlock{}
			}
			if route.Publish.DirectSet {
				return Route{}, p.errAt(dirTok.pos, "duplicate publish.direct")
			}
			if route.Publish.EnabledSet || route.Publish.shorthand {
				return Route{}, p.errAt(dirTok.pos, "publish.direct conflicts with publish shorthand")
			}
			v, quoted, err := p.parseValue()
			if err != nil {
				return Route{}, err
			}
			route.Publish.Direct = v
			route.Publish.DirectQuoted = quoted
			route.Publish.DirectSet = true
			route.Publish.dotNotation = true
		case "publish.managed":
			if route.Publish == nil {
				route.Publish = &PublishBlock{}
			}
			if route.Publish.ManagedSet {
				return Route{}, p.errAt(dirTok.pos, "duplicate publish.managed")
			}
			if route.Publish.EnabledSet || route.Publish.shorthand {
				return Route{}, p.errAt(dirTok.pos, "publish.managed conflicts with publish shorthand")
			}
			v, quoted, err := p.parseValue()
			if err != nil {
				return Route{}, err
			}
			route.Publish.Managed = v
			route.Publish.ManagedQuoted = quoted
			route.Publish.ManagedSet = true
			route.Publish.dotNotation = true
		case "deliver_concurrency":
			if route.DeliverConcurrencySet {
				return Route{}, p.errAt(dirTok.pos, "duplicate route deliver_concurrency")
			}
			v, quoted, err := p.parseValue()
			if err != nil {
				return Route{}, err
			}
			route.DeliverConcurrency = v
			route.DeliverConcurrencyQuoted = quoted
			route.DeliverConcurrencySet = true
		default:
			return Route{}, p.errAt(dirTok.pos, "unknown route directive %q", dirTok.text)
		}
	}

	return route, nil
}

func (p *parser) parseRouteAuthHMACBlock(route *Route) error {
	if route == nil {
		return errors.New("nil route")
	}
	if _, err := p.expect(tokLBrace, "expected '{' after auth hmac"); err != nil {
		return err
	}

	for {
		tok, err := p.peek()
		if err != nil {
			return err
		}
		if tok.kind == tokEOF {
			return p.errAt(tok.pos, "unexpected EOF (missing '}')")
		}
		if tok.kind == tokRBrace {
			_, _ = p.next()
			break
		}
		if tok.kind == tokComment {
			_, _ = p.next()
			continue
		}

		dirTok, _ := p.next()
		if dirTok.kind != tokIdent {
			return p.errAt(dirTok.pos, "expected directive name")
		}
		switch dirTok.text {
		case "secret":
			for {
				v, quoted, err := p.parseValue()
				if err != nil {
					return err
				}
				route.AuthHMACSecrets = append(route.AuthHMACSecrets, v)
				route.AuthHMACSecretsQuoted = append(route.AuthHMACSecretsQuoted, quoted)
				route.AuthHMACSecretIsRef = append(route.AuthHMACSecretIsRef, false)

				next, err := p.peek()
				if err != nil {
					return err
				}
				if next.kind == tokEOF || next.kind == tokRBrace || next.kind == tokComment {
					break
				}
				if next.kind == tokIdent && isRouteAuthHMACDirective(next.text) {
					break
				}
			}
		case "secret_ref":
			for {
				v, quoted, err := p.parseValue()
				if err != nil {
					return err
				}
				route.AuthHMACSecrets = append(route.AuthHMACSecrets, v)
				route.AuthHMACSecretsQuoted = append(route.AuthHMACSecretsQuoted, quoted)
				route.AuthHMACSecretIsRef = append(route.AuthHMACSecretIsRef, true)

				next, err := p.peek()
				if err != nil {
					return err
				}
				if next.kind == tokEOF || next.kind == tokRBrace || next.kind == tokComment {
					break
				}
				if next.kind == tokIdent && isRouteAuthHMACDirective(next.text) {
					break
				}
			}
		case "signature_header":
			if route.AuthHMACSignatureHeaderSet {
				return p.errAt(dirTok.pos, "duplicate route auth hmac signature_header")
			}
			v, quoted, err := p.parseValue()
			if err != nil {
				return err
			}
			route.AuthHMACSignatureHeader = v
			route.AuthHMACSignatureHeaderQuoted = quoted
			route.AuthHMACSignatureHeaderSet = true
		case "timestamp_header":
			if route.AuthHMACTimestampHeaderSet {
				return p.errAt(dirTok.pos, "duplicate route auth hmac timestamp_header")
			}
			v, quoted, err := p.parseValue()
			if err != nil {
				return err
			}
			route.AuthHMACTimestampHeader = v
			route.AuthHMACTimestampHeaderQuoted = quoted
			route.AuthHMACTimestampHeaderSet = true
		case "nonce_header":
			if route.AuthHMACNonceHeaderSet {
				return p.errAt(dirTok.pos, "duplicate route auth hmac nonce_header")
			}
			v, quoted, err := p.parseValue()
			if err != nil {
				return err
			}
			route.AuthHMACNonceHeader = v
			route.AuthHMACNonceHeaderQuoted = quoted
			route.AuthHMACNonceHeaderSet = true
		case "tolerance":
			if route.AuthHMACToleranceSet {
				return p.errAt(dirTok.pos, "duplicate route auth hmac tolerance")
			}
			v, quoted, err := p.parseValue()
			if err != nil {
				return err
			}
			route.AuthHMACTolerance = v
			route.AuthHMACToleranceQuoted = quoted
			route.AuthHMACToleranceSet = true
		default:
			return p.errAt(dirTok.pos, "unknown route auth hmac directive %q", dirTok.text)
		}
	}

	return nil
}

func isRouteAuthHMACDirective(name string) bool {
	switch strings.TrimSpace(name) {
	case "secret", "secret_ref", "signature_header", "timestamp_header", "nonce_header", "tolerance":
		return true
	default:
		return false
	}
}

func (p *parser) parseRouteAuthForwardBlock(block *ForwardAuthBlock) error {
	if block == nil {
		return errors.New("nil forward auth block")
	}
	if _, err := p.expect(tokLBrace, "expected '{' after auth forward URL"); err != nil {
		return err
	}
	block.BlockSet = true

	for {
		tok, err := p.peek()
		if err != nil {
			return err
		}
		if tok.kind == tokEOF {
			return p.errAt(tok.pos, "unexpected EOF (missing '}')")
		}
		if tok.kind == tokRBrace {
			_, _ = p.next()
			break
		}
		if tok.kind == tokComment {
			_, _ = p.next()
			continue
		}

		dirTok, _ := p.next()
		if dirTok.kind != tokIdent {
			return p.errAt(dirTok.pos, "expected directive name")
		}
		switch dirTok.text {
		case "timeout":
			if block.TimeoutSet {
				return p.errAt(dirTok.pos, "duplicate route auth forward timeout")
			}
			v, quoted, err := p.parseValue()
			if err != nil {
				return err
			}
			block.Timeout = v
			block.TimeoutQuoted = quoted
			block.TimeoutSet = true
		case "copy_headers":
			for {
				v, quoted, err := p.parseValue()
				if err != nil {
					return err
				}
				block.CopyHeaders = append(block.CopyHeaders, v)
				block.CopyHeadersQuoted = append(block.CopyHeadersQuoted, quoted)

				next, err := p.peek()
				if err != nil {
					return err
				}
				if next.kind == tokEOF || next.kind == tokRBrace || next.kind == tokComment {
					break
				}
				if next.kind == tokIdent && isRouteAuthForwardDirective(next.text) {
					break
				}
			}
		case "body_limit":
			if block.BodyLimitSet {
				return p.errAt(dirTok.pos, "duplicate route auth forward body_limit")
			}
			v, quoted, err := p.parseValue()
			if err != nil {
				return err
			}
			block.BodyLimit = v
			block.BodyLimitQuoted = quoted
			block.BodyLimitSet = true
		default:
			return p.errAt(dirTok.pos, "unknown route auth forward directive %q", dirTok.text)
		}
	}

	return nil
}

func isRouteAuthForwardDirective(name string) bool {
	switch strings.TrimSpace(name) {
	case "timeout", "copy_headers", "body_limit":
		return true
	default:
		return false
	}
}

func (p *parser) parseMatchBlock() (*MatchBlock, error) {
	if _, err := p.expect(tokLBrace, "expected '{' after match"); err != nil {
		return nil, err
	}
	return p.parseMatchBody()
}

func (p *parser) parseMatchBody() (*MatchBlock, error) {
	out := &MatchBlock{}

	for {
		tok, err := p.peek()
		if err != nil {
			return nil, err
		}
		if tok.kind == tokEOF {
			return nil, p.errAt(tok.pos, "unexpected EOF (missing '}')")
		}
		if tok.kind == tokRBrace {
			_, _ = p.next()
			break
		}
		if tok.kind == tokComment {
			_, _ = p.next()
			continue
		}

		dirTok, _ := p.next()
		if dirTok.kind != tokIdent {
			return nil, p.errAt(dirTok.pos, "expected directive name")
		}
		switch dirTok.text {
		case "method":
			for {
				v, quoted, err := p.parseValue()
				if err != nil {
					return nil, err
				}
				out.Methods = append(out.Methods, v)
				out.MethodsQuoted = append(out.MethodsQuoted, quoted)

				next, err := p.peek()
				if err != nil {
					return nil, err
				}
				if next.kind == tokEOF || next.kind == tokRBrace || next.kind == tokComment {
					break
				}
				if next.kind == tokIdent && isMatchDirective(next.text) {
					break
				}
			}
		case "host":
			for {
				v, quoted, err := p.parseValue()
				if err != nil {
					return nil, err
				}
				out.Hosts = append(out.Hosts, v)
				out.HostsQuoted = append(out.HostsQuoted, quoted)

				next, err := p.peek()
				if err != nil {
					return nil, err
				}
				if next.kind == tokEOF || next.kind == tokRBrace || next.kind == tokComment {
					break
				}
				if next.kind == tokIdent && isMatchDirective(next.text) {
					break
				}
			}
		case "header":
			for {
				name, nameQuoted, err := p.parseValue()
				if err != nil {
					return nil, err
				}
				value, valueQuoted, err := p.parseValue()
				if err != nil {
					return nil, err
				}
				out.Headers = append(out.Headers, HeaderMatch{
					Name:        name,
					NameQuoted:  nameQuoted,
					Value:       value,
					ValueQuoted: valueQuoted,
				})

				next, err := p.peek()
				if err != nil {
					return nil, err
				}
				if next.kind == tokEOF || next.kind == tokRBrace || next.kind == tokComment {
					break
				}
				if next.kind == tokIdent && isMatchDirective(next.text) {
					break
				}
			}
		case "header_exists":
			for {
				v, quoted, err := p.parseValue()
				if err != nil {
					return nil, err
				}
				out.HeaderExists = append(out.HeaderExists, v)
				out.HeaderExistsQuoted = append(out.HeaderExistsQuoted, quoted)

				next, err := p.peek()
				if err != nil {
					return nil, err
				}
				if next.kind == tokEOF || next.kind == tokRBrace || next.kind == tokComment {
					break
				}
				if next.kind == tokIdent && isMatchDirective(next.text) {
					break
				}
			}
		case "query":
			for {
				name, nameQuoted, err := p.parseValue()
				if err != nil {
					return nil, err
				}
				value, valueQuoted, err := p.parseValue()
				if err != nil {
					return nil, err
				}
				out.Query = append(out.Query, QueryMatch{
					Name:        name,
					NameQuoted:  nameQuoted,
					Value:       value,
					ValueQuoted: valueQuoted,
				})

				next, err := p.peek()
				if err != nil {
					return nil, err
				}
				if next.kind == tokEOF || next.kind == tokRBrace || next.kind == tokComment {
					break
				}
				if next.kind == tokIdent && isMatchDirective(next.text) {
					break
				}
			}
		case "remote_ip":
			for {
				v, quoted, err := p.parseValue()
				if err != nil {
					return nil, err
				}
				out.RemoteIPs = append(out.RemoteIPs, v)
				out.RemoteIPsQuoted = append(out.RemoteIPsQuoted, quoted)

				next, err := p.peek()
				if err != nil {
					return nil, err
				}
				if next.kind == tokEOF || next.kind == tokRBrace || next.kind == tokComment {
					break
				}
				if next.kind == tokIdent && isMatchDirective(next.text) {
					break
				}
			}
		case "query_exists":
			for {
				v, quoted, err := p.parseValue()
				if err != nil {
					return nil, err
				}
				out.QueryExists = append(out.QueryExists, v)
				out.QueryExistsQuoted = append(out.QueryExistsQuoted, quoted)

				next, err := p.peek()
				if err != nil {
					return nil, err
				}
				if next.kind == tokEOF || next.kind == tokRBrace || next.kind == tokComment {
					break
				}
				if next.kind == tokIdent && isMatchDirective(next.text) {
					break
				}
			}
		default:
			return nil, p.errAt(dirTok.pos, "unknown match directive %q", dirTok.text)
		}
	}

	return out, nil
}

func isMatchDirective(name string) bool {
	switch strings.TrimSpace(name) {
	case "method", "host", "header", "header_exists", "query", "remote_ip", "query_exists":
		return true
	default:
		return false
	}
}

func (p *parser) parseNamedMatcherBlock() (NamedMatcher, error) {
	nameTok, _ := p.next()
	if nameTok.kind != tokIdent || !strings.HasPrefix(nameTok.text, "@") {
		return NamedMatcher{}, p.errAt(nameTok.pos, "expected matcher name starting with '@'")
	}
	name := strings.TrimPrefix(nameTok.text, "@")
	if name == "" {
		return NamedMatcher{}, p.errAt(nameTok.pos, "matcher name must not be empty")
	}

	if _, err := p.expect(tokLBrace, "expected '{' after %s", nameTok.text); err != nil {
		return NamedMatcher{}, err
	}
	match, err := p.parseMatchBody()
	if err != nil {
		return NamedMatcher{}, err
	}
	return NamedMatcher{Name: name, Match: match}, nil
}

func (p *parser) parsePullBlock() (*Pull, error) {
	if _, err := p.expect(tokLBrace, "expected '{' after pull"); err != nil {
		return nil, err
	}

	out := &Pull{}
	pathSet := false
	for {
		tok, err := p.peek()
		if err != nil {
			return nil, err
		}
		if tok.kind == tokEOF {
			return nil, p.errAt(tok.pos, "unexpected EOF (missing '}')")
		}
		if tok.kind == tokRBrace {
			_, _ = p.next()
			break
		}
		if tok.kind == tokComment {
			_, _ = p.next()
			continue
		}

		dirTok, _ := p.next()
		if dirTok.kind != tokIdent {
			return nil, p.errAt(dirTok.pos, "expected directive name")
		}
		switch dirTok.text {
		case "path":
			if pathSet {
				return nil, p.errAt(dirTok.pos, "duplicate pull path")
			}
			v, quoted, err := p.parseValue()
			if err != nil {
				return nil, err
			}
			out.Path = v
			out.PathQuoted = quoted
			pathSet = true
		case "auth":
			typTok, err := p.expect(tokIdent, "expected auth type after pull.auth")
			if err != nil {
				return nil, err
			}
			switch typTok.text {
			case "token":
				v, quoted, err := p.parseValue()
				if err != nil {
					return nil, err
				}
				out.AuthTokens = append(out.AuthTokens, v)
				out.AuthTokensQuoted = append(out.AuthTokensQuoted, quoted)
			default:
				return nil, p.errAt(typTok.pos, "unknown pull auth type %q", typTok.text)
			}
		default:
			return nil, p.errAt(dirTok.pos, "unknown pull directive %q", dirTok.text)
		}
	}

	return out, nil
}

func (p *parser) parseQueueBlock() (*QueueBlock, error) {
	if _, err := p.expect(tokLBrace, "expected '{' after queue"); err != nil {
		return nil, err
	}

	out := &QueueBlock{}
	for {
		tok, err := p.peek()
		if err != nil {
			return nil, err
		}
		if tok.kind == tokEOF {
			return nil, p.errAt(tok.pos, "unexpected EOF (missing '}')")
		}
		if tok.kind == tokRBrace {
			_, _ = p.next()
			break
		}
		if tok.kind == tokComment {
			_, _ = p.next()
			continue
		}

		dirTok, _ := p.next()
		if dirTok.kind != tokIdent {
			return nil, p.errAt(dirTok.pos, "expected directive name")
		}
		switch dirTok.text {
		case "backend":
			if out.BackendSet {
				return nil, p.errAt(dirTok.pos, "duplicate queue backend")
			}
			v, quoted, err := p.parseValue()
			if err != nil {
				return nil, err
			}
			out.Backend = v
			out.BackendQuoted = quoted
			out.BackendSet = true
		default:
			return nil, p.errAt(dirTok.pos, "unknown queue directive %q", dirTok.text)
		}
	}

	return out, nil
}

func (p *parser) parseDeliverBlock() (*Deliver, error) {
	if _, err := p.expect(tokLBrace, "expected '{' after deliver target"); err != nil {
		return nil, err
	}

	out := &Deliver{}
	for {
		tok, err := p.peek()
		if err != nil {
			return nil, err
		}
		if tok.kind == tokEOF {
			return nil, p.errAt(tok.pos, "unexpected EOF (missing '}')")
		}
		if tok.kind == tokRBrace {
			_, _ = p.next()
			break
		}
		if tok.kind == tokComment {
			_, _ = p.next()
			continue
		}

		dirTok, _ := p.next()
		if dirTok.kind != tokIdent {
			return nil, p.errAt(dirTok.pos, "expected directive name")
		}
		switch dirTok.text {
		case "retry":
			if out.Retry != nil {
				return nil, p.errAt(dirTok.pos, "duplicate retry directive")
			}
			r, err := p.parseRetryDirective()
			if err != nil {
				return nil, err
			}
			out.Retry = r
		case "timeout":
			if out.TimeoutSet {
				return nil, p.errAt(dirTok.pos, "duplicate timeout directive")
			}
			v, quoted, err := p.parseValue()
			if err != nil {
				return nil, err
			}
			out.Timeout = v
			out.TimeoutQuoted = quoted
			out.TimeoutSet = true
		case "sign":
			typTok, err := p.expect(tokIdent, "expected sign type after deliver.sign")
			if err != nil {
				return nil, err
			}
			switch typTok.text {
			case "hmac":
				nextTok, err := p.peek()
				if err != nil {
					return nil, err
				}
				if nextTok.kind == tokIdent && nextTok.text == "secret_ref" {
					if out.SignHMACSecretSet {
						return nil, p.errAt(typTok.pos, "duplicate deliver sign hmac")
					}
					_, _ = p.next()
					for {
						v, quoted, err := p.parseValue()
						if err != nil {
							return nil, err
						}
						out.SignHMACSecretRefs = append(out.SignHMACSecretRefs, v)
						out.SignHMACSecretRefsQuoted = append(out.SignHMACSecretRefsQuoted, quoted)

						next, err := p.peek()
						if err != nil {
							return nil, err
						}
						if next.kind == tokEOF || next.kind == tokRBrace || next.kind == tokComment {
							break
						}
						if next.kind == tokIdent && isDeliverDirective(next.text) {
							break
						}
					}
				} else {
					if out.SignHMACSecretSet || len(out.SignHMACSecretRefs) > 0 {
						return nil, p.errAt(typTok.pos, "duplicate deliver sign hmac")
					}
					v, quoted, err := p.parseValue()
					if err != nil {
						return nil, err
					}
					out.SignHMACSecret = v
					out.SignHMACSecretQuoted = quoted
					out.SignHMACSecretSet = true
				}
			case "signature_header":
				if out.SignHMACSignatureHeaderSet {
					return nil, p.errAt(typTok.pos, "duplicate deliver sign signature_header")
				}
				v, quoted, err := p.parseValue()
				if err != nil {
					return nil, err
				}
				out.SignHMACSignatureHeader = v
				out.SignHMACSignatureHeaderQuoted = quoted
				out.SignHMACSignatureHeaderSet = true
			case "timestamp_header":
				if out.SignHMACTimestampHeaderSet {
					return nil, p.errAt(typTok.pos, "duplicate deliver sign timestamp_header")
				}
				v, quoted, err := p.parseValue()
				if err != nil {
					return nil, err
				}
				out.SignHMACTimestampHeader = v
				out.SignHMACTimestampHeaderQuoted = quoted
				out.SignHMACTimestampHeaderSet = true
			case "secret_selection":
				if out.SignHMACSecretSelectionSet {
					return nil, p.errAt(typTok.pos, "duplicate deliver sign secret_selection")
				}
				v, quoted, err := p.parseValue()
				if err != nil {
					return nil, err
				}
				out.SignHMACSecretSelection = v
				out.SignHMACSecretSelectionQuoted = quoted
				out.SignHMACSecretSelectionSet = true
			default:
				return nil, p.errAt(typTok.pos, "unknown deliver sign type %q", typTok.text)
			}
		default:
			return nil, p.errAt(dirTok.pos, "unknown deliver directive %q", dirTok.text)
		}
	}

	return out, nil
}

func (p *parser) parseRetryDirective() (*RetryBlock, error) {
	typ, quoted, err := p.parseValue()
	if err != nil {
		return nil, err
	}
	out := &RetryBlock{
		Type:       typ,
		TypeQuoted: quoted,
	}

	for {
		tok, err := p.peek()
		if err != nil {
			return nil, err
		}
		if tok.kind != tokIdent {
			break
		}

		switch tok.text {
		case "max":
			_, _ = p.next()
			if out.MaxSet {
				return nil, p.errAt(tok.pos, "duplicate retry max")
			}
			v, quoted, err := p.parseValue()
			if err != nil {
				return nil, err
			}
			out.Max = v
			out.MaxQuoted = quoted
			out.MaxSet = true
		case "base":
			_, _ = p.next()
			if out.BaseSet {
				return nil, p.errAt(tok.pos, "duplicate retry base")
			}
			v, quoted, err := p.parseValue()
			if err != nil {
				return nil, err
			}
			out.Base = v
			out.BaseQuoted = quoted
			out.BaseSet = true
		case "cap":
			_, _ = p.next()
			if out.CapSet {
				return nil, p.errAt(tok.pos, "duplicate retry cap")
			}
			v, quoted, err := p.parseValue()
			if err != nil {
				return nil, err
			}
			out.Cap = v
			out.CapQuoted = quoted
			out.CapSet = true
		case "jitter":
			_, _ = p.next()
			if out.JitterSet {
				return nil, p.errAt(tok.pos, "duplicate retry jitter")
			}
			v, quoted, err := p.parseValue()
			if err != nil {
				return nil, err
			}
			out.Jitter = v
			out.JitterQuoted = quoted
			out.JitterSet = true
		default:
			return out, nil
		}
	}

	return out, nil
}

func isDeliverDirective(name string) bool {
	switch strings.TrimSpace(name) {
	case "retry", "timeout", "sign":
		return true
	default:
		return false
	}
}

func (p *parser) parseValue() (string, bool, error) {
	tok, _ := p.next()
	switch tok.kind {
	case tokString, tokIdent:
		return tok.text, tok.kind == tokString, nil
	default:
		return "", false, p.errAt(tok.pos, "expected value")
	}
}

func (p *parser) peek() (token, error) {
	if p.hasPeek {
		return p.peeked, nil
	}
	tok, err := p.lex.nextToken()
	if err != nil {
		return token{}, err
	}
	p.peeked = tok
	p.hasPeek = true
	return tok, nil
}

func (p *parser) next() (token, error) {
	if p.hasPeek {
		p.hasPeek = false
		return p.peeked, nil
	}
	return p.lex.nextToken()
}

func (p *parser) expect(kind tokenKind, msg string, args ...any) (token, error) {
	tok, err := p.next()
	if err != nil {
		return token{}, err
	}
	if tok.kind != kind {
		return token{}, p.errAt(tok.pos, msg, args...)
	}
	return tok, nil
}

func (p *parser) errAt(pos position, format string, args ...any) error {
	msg := fmt.Sprintf(format, args...)
	return fmt.Errorf("config parse error at %s: %s", pos.String(), msg)
}
