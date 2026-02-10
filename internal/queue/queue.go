package queue

import "time"

type State string

const (
	StateQueued    State = "queued"
	StateLeased    State = "leased"
	StateDelivered State = "delivered"
	StateDead      State = "dead"
	StateCanceled  State = "canceled"
)

type AttemptOutcome string

const (
	AttemptOutcomeAcked AttemptOutcome = "acked"
	AttemptOutcomeRetry AttemptOutcome = "retry"
	AttemptOutcomeDead  AttemptOutcome = "dead"
)

type Envelope struct {
	ID            string
	Route         string
	Target        string
	State         State
	ReceivedAt    time.Time
	Attempt       int
	NextRunAt     time.Time
	Payload       []byte
	Headers       map[string]string
	Trace         map[string]string
	DeadReason    string
	SchemaVersion int
	LeaseID       string
	LeaseUntil    time.Time
}

type DequeueRequest struct {
	Route       string
	Target      string
	Batch       int
	MaxWait     time.Duration
	LeaseTTL    time.Duration
	Now         time.Time
	AllowLeased bool
}

type DequeueResponse struct {
	Items []Envelope
}

type DeadListRequest struct {
	Route          string
	Limit          int
	Before         time.Time
	IncludePayload bool
	IncludeHeaders bool
	IncludeTrace   bool
}

type DeadListResponse struct {
	Items []Envelope
}

type DeadRequeueRequest struct {
	IDs []string
}

type DeadRequeueResponse struct {
	Requeued int
}

type DeadDeleteRequest struct {
	IDs []string
}

type DeadDeleteResponse struct {
	Deleted int
}

type MessageListRequest struct {
	Route          string
	Target         string
	State          State
	Order          string
	Limit          int
	Before         time.Time
	IncludePayload bool
	IncludeHeaders bool
	IncludeTrace   bool
}

type MessageListResponse struct {
	Items []Envelope
}

type MessageCancelRequest struct {
	IDs []string
}

type MessageCancelResponse struct {
	Canceled    int
	Matched     int
	PreviewOnly bool
}

type MessageRequeueRequest struct {
	IDs []string
}

type MessageRequeueResponse struct {
	Requeued    int
	Matched     int
	PreviewOnly bool
}

type MessageResumeRequest struct {
	IDs []string
}

type MessageResumeResponse struct {
	Resumed     int
	Matched     int
	PreviewOnly bool
}

type MessageLookupRequest struct {
	IDs []string
}

type MessageLookupItem struct {
	ID    string
	Route string
	State State
}

type MessageLookupResponse struct {
	Items []MessageLookupItem
}

type MessageManageFilterRequest struct {
	Route       string
	Target      string
	State       State
	Limit       int
	Before      time.Time
	PreviewOnly bool
}

type DeliveryAttempt struct {
	ID         string
	EventID    string
	Route      string
	Target     string
	Attempt    int
	StatusCode int
	Error      string
	Outcome    AttemptOutcome
	DeadReason string
	CreatedAt  time.Time
}

type AttemptListRequest struct {
	Route   string
	Target  string
	EventID string
	Outcome AttemptOutcome
	Limit   int
	Before  time.Time
}

type AttemptListResponse struct {
	Items []DeliveryAttempt
}

type BacklogTrendSample struct {
	CapturedAt time.Time
	Queued     int
	Leased     int
	Dead       int
}

type BacklogTrendListRequest struct {
	Route  string
	Target string
	Since  time.Time
	Until  time.Time
	Limit  int
}

type BacklogTrendListResponse struct {
	Items     []BacklogTrendSample
	Truncated bool
}

// BacklogTrendStore is an optional extension implemented by stores
// that can persist and query backlog trend snapshots.
type BacklogTrendStore interface {
	CaptureBacklogTrendSample(at time.Time) error
	ListBacklogTrend(req BacklogTrendListRequest) (BacklogTrendListResponse, error)
}

// BatchEnqueuer is an optional extension for transactional batch enqueue.
// When supported, all items are committed atomically (all-or-nothing).
type BatchEnqueuer interface {
	EnqueueBatch(items []Envelope) (int, error)
}

const statsTopBacklogLimit = 10

const (
	MessageOrderDesc = "desc"
	MessageOrderAsc  = "asc"
)

type BacklogBucket struct {
	Route  string
	Target string
	Queued int

	OldestQueuedReceivedAt time.Time
	EarliestQueuedNextRun  time.Time
	OldestQueuedAge        time.Duration
	ReadyLag               time.Duration
}

type Stats struct {
	Total   int
	ByState map[State]int

	OldestQueuedReceivedAt time.Time
	EarliestQueuedNextRun  time.Time
	OldestQueuedAge        time.Duration
	ReadyLag               time.Duration

	TopQueued []BacklogBucket
}

type Store interface {
	Enqueue(env Envelope) error
	Dequeue(req DequeueRequest) (DequeueResponse, error)
	Ack(leaseID string) error
	Nack(leaseID string, delay time.Duration) error
	Extend(leaseID string, extendBy time.Duration) error
	MarkDead(leaseID string, reason string) error
	ListDead(req DeadListRequest) (DeadListResponse, error)
	RequeueDead(req DeadRequeueRequest) (DeadRequeueResponse, error)
	DeleteDead(req DeadDeleteRequest) (DeadDeleteResponse, error)
	ListMessages(req MessageListRequest) (MessageListResponse, error)
	LookupMessages(req MessageLookupRequest) (MessageLookupResponse, error)
	CancelMessages(req MessageCancelRequest) (MessageCancelResponse, error)
	RequeueMessages(req MessageRequeueRequest) (MessageRequeueResponse, error)
	ResumeMessages(req MessageResumeRequest) (MessageResumeResponse, error)
	CancelMessagesByFilter(req MessageManageFilterRequest) (MessageCancelResponse, error)
	RequeueMessagesByFilter(req MessageManageFilterRequest) (MessageRequeueResponse, error)
	ResumeMessagesByFilter(req MessageManageFilterRequest) (MessageResumeResponse, error)
	Stats() (Stats, error)
	RecordAttempt(attempt DeliveryAttempt) error
	ListAttempts(req AttemptListRequest) (AttemptListResponse, error)
}
