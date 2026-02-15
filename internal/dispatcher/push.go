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
	ObserveDead    func(reason string)

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
		routeLeaseTTL := routeLeaseTTL(rt.Targets, leaseSlack)
		targetsByURL := targetConfigByURL(rt.Targets)
		dequeueBatch := routeDequeueBatch(concurrency, len(rt.Targets))
		for w := 0; w < concurrency; w++ {
			d.wg.Add(1)
			go d.runRoute(logger, rt.Route, targetsByURL, maxWait, routeLeaseTTL, dequeueBatch)
		}
	}
}

func targetConfigByURL(targets []TargetConfig) map[string]TargetConfig {
	byURL := make(map[string]TargetConfig, len(targets))
	for _, target := range targets {
		byURL[target.URL] = target
	}
	return byURL
}

func routeLeaseTTL(targets []TargetConfig, leaseSlack time.Duration) time.Duration {
	maxTimeout := time.Duration(0)
	for _, target := range targets {
		timeout := target.Timeout
		if timeout <= 0 {
			timeout = 10 * time.Second
		}
		if timeout > maxTimeout {
			maxTimeout = timeout
		}
	}
	ttl := maxTimeout + leaseSlack
	if ttl < 30*time.Second {
		ttl = 30 * time.Second
	}
	return ttl
}

func routeDequeueBatch(concurrency int, targetCount int) int {
	if concurrency <= 1 {
		return 1
	}
	// Multi-target routes favor fairness and lower lease hold time per worker
	// over dequeue roundtrip reduction.
	if targetCount > 1 {
		if concurrency >= 2 {
			return 2
		}
		return 1
	}
	if concurrency >= 4 {
		return 4
	}
	return concurrency
}

func routeMutationBatch(dequeueBatch int) int {
	if dequeueBatch <= 1 {
		return 1
	}
	// Single-target routes can safely apply lease mutations in the same batch
	// size as dequeue (bounded by routeDequeueBatch) to cut store roundtrips.
	if dequeueBatch > 4 {
		return 4
	}
	return dequeueBatch
}

type leaseActionKind int

const (
	leaseActionAck leaseActionKind = iota
	leaseActionNack
	leaseActionMarkDead
)

type leaseAction struct {
	kind             leaseActionKind
	route            string
	target           string
	leaseID          string
	delay            time.Duration
	reason           string
	tolerateNotFound bool
}

func (d *PushDispatcher) runRoute(
	logger *slog.Logger,
	route string,
	targetsByURL map[string]TargetConfig,
	maxWait time.Duration,
	leaseTTL time.Duration,
	dequeueBatch int,
) {
	defer d.wg.Done()

	if logger == nil {
		logger = slog.Default()
	}

	const missingTargetBackoff = time.Second
	if dequeueBatch <= 0 {
		dequeueBatch = 1
	}
	useBatchMutations := len(targetsByURL) == 1

	for {
		select {
		case <-d.stopCh:
			return
		default:
		}

		resp, err := d.Store.Dequeue(queue.DequeueRequest{
			Route:    route,
			Batch:    dequeueBatch,
			MaxWait:  maxWait,
			LeaseTTL: leaseTTL,
		})
		if err != nil {
			logger.Warn("dispatcher_dequeue_failed",
				slog.String("route", route),
				slog.String("target", "<any>"),
				slog.Any("err", err),
			)
			time.Sleep(200 * time.Millisecond)
			continue
		}
		if len(resp.Items) == 0 {
			continue
		}

		mutationBatch := routeMutationBatch(dequeueBatch)
		actions := make([]leaseAction, 0, mutationBatch)
		for i, env := range resp.Items {
			select {
			case <-d.stopCh:
				d.requeueLeases(logger, resp.Items[i:], 0, "dispatcher_stop_requeue_failed")
				return
			default:
			}

			target, ok := targetsByURL[env.Target]
			if !ok {
				logger.Warn("dispatcher_target_config_missing",
					slog.String("route", env.Route),
					slog.String("target", env.Target),
					slog.String("lease_id", env.LeaseID),
				)
				d.applyLeaseAction(logger, leaseAction{
					kind:             leaseActionNack,
					route:            env.Route,
					target:           env.Target,
					leaseID:          env.LeaseID,
					delay:            missingTargetBackoff,
					tolerateNotFound: true,
				})
				continue
			}
			action := d.classifyDelivery(logger, env, target)
			if !useBatchMutations {
				d.applyLeaseAction(logger, action)
				continue
			}
			actions = append(actions, action)
			if len(actions) >= mutationBatch {
				d.applyLeaseActions(logger, actions)
				actions = actions[:0]
			}
		}

		if useBatchMutations && len(actions) > 0 {
			d.applyLeaseActions(logger, actions)
		}
	}
}

func (d *PushDispatcher) requeueLeases(logger *slog.Logger, items []queue.Envelope, delay time.Duration, warnMsg string) {
	for _, env := range items {
		if err := d.Store.Nack(env.LeaseID, delay); err != nil && !errors.Is(err, queue.ErrLeaseExpired) && !errors.Is(err, queue.ErrLeaseNotFound) {
			logger.Warn(warnMsg,
				slog.String("route", env.Route),
				slog.String("target", env.Target),
				slog.String("lease_id", env.LeaseID),
				slog.Any("err", err),
			)
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

func (d *PushDispatcher) handleDelivery(logger *slog.Logger, env queue.Envelope, target TargetConfig) {
	if logger == nil {
		logger = slog.Default()
	}

	action := d.classifyDelivery(logger, env, target)
	d.applyLeaseAction(logger, action)
}

func (d *PushDispatcher) classifyDelivery(logger *slog.Logger, env queue.Envelope, target TargetConfig) leaseAction {
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
		return leaseAction{
			kind:    leaseActionAck,
			route:   env.Route,
			target:  target.URL,
			leaseID: env.LeaseID,
		}
	}

	shouldRetry := shouldRetry(res)
	// retry.max is defined as maximum retry attempts (not total attempts).
	// Attempt starts at 1 for the first delivery, so retry is allowed while
	// current attempt number is <= retry.max.
	if shouldRetry && env.Attempt <= target.Retry.Max {
		delay := retryDelay(env.Attempt, target.Retry)
		attempt.Outcome = queue.AttemptOutcomeRetry
		d.recordAttempt(logger, attempt)
		return leaseAction{
			kind:    leaseActionNack,
			route:   env.Route,
			target:  target.URL,
			leaseID: env.LeaseID,
			delay:   delay,
		}
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
	return leaseAction{
		kind:    leaseActionMarkDead,
		route:   env.Route,
		target:  target.URL,
		leaseID: env.LeaseID,
		reason:  reason,
	}
}

func (d *PushDispatcher) applyLeaseActions(logger *slog.Logger, actions []leaseAction) {
	if len(actions) == 0 {
		return
	}

	batchStore, ok := d.Store.(queue.LeaseBatchStore)
	if !ok {
		for _, action := range actions {
			d.applyLeaseAction(logger, action)
		}
		return
	}

	d.applyLeaseActionsBatch(logger, batchStore, actions)
}

func (d *PushDispatcher) applyLeaseActionsBatch(logger *slog.Logger, batchStore queue.LeaseBatchStore, actions []leaseAction) {
	acks := make([]leaseAction, 0, len(actions))
	nacksByDelay := make(map[time.Duration][]leaseAction)
	deadByReason := make(map[string][]leaseAction)

	for _, action := range actions {
		switch action.kind {
		case leaseActionAck:
			acks = append(acks, action)
		case leaseActionNack:
			nacksByDelay[action.delay] = append(nacksByDelay[action.delay], action)
		case leaseActionMarkDead:
			deadByReason[action.reason] = append(deadByReason[action.reason], action)
		}
	}

	if len(acks) > 0 {
		ids := leaseIDsFromActions(acks)
		res, err := batchStore.AckBatch(ids)
		if err != nil {
			logger.Warn("dispatcher_ack_batch_failed",
				slog.Int("batch", len(ids)),
				slog.Any("err", err),
			)
			for _, action := range acks {
				d.applyLeaseAction(logger, action)
			}
		} else {
			d.logBatchConflicts(logger, "dispatcher_ack_failed", acks, res.Conflicts)
		}
	}

	for delay, grouped := range nacksByDelay {
		ids := leaseIDsFromActions(grouped)
		res, err := batchStore.NackBatch(ids, delay)
		if err != nil {
			logger.Warn("dispatcher_nack_batch_failed",
				slog.Int("batch", len(ids)),
				slog.Duration("delay", delay),
				slog.Any("err", err),
			)
			for _, action := range grouped {
				d.applyLeaseAction(logger, action)
			}
			continue
		}
		d.logBatchConflicts(logger, "dispatcher_nack_failed", grouped, res.Conflicts)
	}

	for reason, grouped := range deadByReason {
		ids := leaseIDsFromActions(grouped)
		res, err := batchStore.MarkDeadBatch(ids, reason)
		if err != nil {
			logger.Warn("dispatcher_mark_dead_batch_failed",
				slog.Int("batch", len(ids)),
				slog.String("reason", reason),
				slog.Any("err", err),
			)
			for _, action := range grouped {
				d.applyLeaseAction(logger, action)
			}
			continue
		}
		d.logBatchConflicts(logger, "dispatcher_mark_dead_failed", grouped, res.Conflicts)
	}
}

func leaseIDsFromActions(actions []leaseAction) []string {
	out := make([]string, 0, len(actions))
	for _, action := range actions {
		out = append(out, action.leaseID)
	}
	return out
}

func (d *PushDispatcher) logBatchConflicts(
	logger *slog.Logger,
	warnMsg string,
	actions []leaseAction,
	conflicts []queue.LeaseBatchConflict,
) {
	if len(conflicts) == 0 {
		return
	}

	byLease := make(map[string]leaseAction, len(actions))
	for _, action := range actions {
		byLease[action.leaseID] = action
	}
	for _, conflict := range conflicts {
		if conflict.Expired {
			continue
		}
		action, ok := byLease[conflict.LeaseID]
		if !ok {
			continue
		}
		logger.Warn(warnMsg,
			slog.String("route", action.route),
			slog.String("target", action.target),
			slog.String("lease_id", conflict.LeaseID),
			slog.Any("err", queue.ErrLeaseNotFound),
		)
	}
}

func (d *PushDispatcher) applyLeaseAction(logger *slog.Logger, action leaseAction) {
	switch action.kind {
	case leaseActionAck:
		if err := d.Store.Ack(action.leaseID); err != nil && !errors.Is(err, queue.ErrLeaseExpired) {
			logger.Warn("dispatcher_ack_failed",
				slog.String("route", action.route),
				slog.String("target", action.target),
				slog.String("lease_id", action.leaseID),
				slog.Any("err", err),
			)
		}
	case leaseActionNack:
		err := d.Store.Nack(action.leaseID, action.delay)
		if err != nil && !errors.Is(err, queue.ErrLeaseExpired) && (!action.tolerateNotFound || !errors.Is(err, queue.ErrLeaseNotFound)) {
			logger.Warn("dispatcher_nack_failed",
				slog.String("route", action.route),
				slog.String("target", action.target),
				slog.String("lease_id", action.leaseID),
				slog.Any("err", err),
			)
		}
	case leaseActionMarkDead:
		if err := d.Store.MarkDead(action.leaseID, action.reason); err != nil && !errors.Is(err, queue.ErrLeaseExpired) {
			logger.Warn("dispatcher_mark_dead_failed",
				slog.String("route", action.route),
				slog.String("target", action.target),
				slog.String("lease_id", action.leaseID),
				slog.Any("err", err),
			)
		}
	}
}

func (d *PushDispatcher) recordAttempt(logger *slog.Logger, attempt queue.DeliveryAttempt) {
	if d.ObserveAttempt != nil {
		d.ObserveAttempt(attempt.Outcome)
	}
	if d.ObserveDead != nil && attempt.Outcome == queue.AttemptOutcomeDead {
		d.ObserveDead(attempt.DeadReason)
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
