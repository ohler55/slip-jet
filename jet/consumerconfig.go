// Copyright (c) 2024, Peter Ohler, All rights reserved.

package jet

import (
	"strings"

	"github.com/nats-io/nats.go/jetstream"
	"github.com/ohler55/slip"
)

type consumerOpt struct {
	doc    *slip.DocArg
	update func(config *jetstream.ConsumerConfig, v slip.Object)
}

var consumerOptMap = map[string]*consumerOpt{
	":name": {
		doc: &slip.DocArg{
			Name: "name",
			Type: "string",
			Text: `Name is an optional name for the consumer. If not set, one is
generated automatically. The name cannot contain whitespace, ., *, >, path
separators (forward or backwards slash), and non-printable characters.`,
		},
		update: func(config *jetstream.ConsumerConfig, v slip.Object) {
			config.Name = slip.MustBeString(v, ":name")
		},
	},
	":durable": {
		doc: &slip.DocArg{
			Name: "durable",
			Type: "string",
			Text: `Durable is an optional durable name for the consumer. If both
_:durable_ and _:name_ are set, they have to be equal. Unless _:inactive-threshold_
is set, a durable consumer will not be cleaned up automatically. Durable cannot
contain whitespace, ., *, >, path separators (forward or backwards slash), and
non-printable characters.`,
		},
		update: func(config *jetstream.ConsumerConfig, v slip.Object) {
			config.Durable = slip.MustBeString(v, ":durable")
		},
	},

	// // Description provides an optional description of the consumer.
	// Description string `json:"description,omitempty"`

	// // DeliverPolicy defines from which point to start delivering messages
	// // from the stream. Defaults to DeliverAllPolicy.
	// DeliverPolicy DeliverPolicy `json:"deliver_policy"`

	// // OptStartSeq is an optional sequence number from which to start
	// // message delivery. Only applicable when DeliverPolicy is set to
	// // DeliverByStartSequencePolicy.
	// OptStartSeq uint64 `json:"opt_start_seq,omitempty"`

	// // OptStartTime is an optional time from which to start message
	// // delivery. Only applicable when DeliverPolicy is set to
	// // DeliverByStartTimePolicy.
	// OptStartTime *time.Time `json:"opt_start_time,omitempty"`

	// // AckPolicy defines the acknowledgement policy for the consumer.
	// // Defaults to AckExplicitPolicy.
	// AckPolicy AckPolicy `json:"ack_policy"`

	// // AckWait defines how long the server will wait for an acknowledgement
	// // before resending a message. If not set, server default is 30 seconds.
	// AckWait time.Duration `json:"ack_wait,omitempty"`

	// // MaxDeliver defines the maximum number of delivery attempts for a
	// // message. Applies to any message that is re-sent due to ack policy.
	// //  If not set, server default is -1 (unlimited).
	// MaxDeliver int `json:"max_deliver,omitempty"`

	// // BackOff specifies the optional back-off intervals for retrying
	// // message delivery after a failed acknowledgement. It overrides
	// // AckWait.
	// //
	// // BackOff only applies to messages not acknowledged in specified time,
	// // not messages that were nack'ed.
	// //
	// // The number of intervals specified must be lower or equal to
	// // MaxDeliver. If the number of intervals is lower, the last interval is
	// // used for all remaining attempts.
	// BackOff []time.Duration `json:"backoff,omitempty"`

	// // FilterSubject can be used to filter messages delivered from the
	// // stream. FilterSubject is exclusive with FilterSubjects.
	// FilterSubject string `json:"filter_subject,omitempty"`

	// // ReplayPolicy defines the rate at which messages are sent to the
	// // consumer. If ReplayOriginalPolicy is set, messages are sent in the
	// // same intervals in which they were stored on stream. This can be used
	// // e.g. to simulate production traffic in development environments. If
	// // ReplayInstantPolicy is set, messages are sent as fast as possible.
	// // Defaults to ReplayInstantPolicy.
	// ReplayPolicy ReplayPolicy `json:"replay_policy"`

	// // RateLimit specifies an optional maximum rate of message delivery in
	// // bits per second.
	// RateLimit uint64 `json:"rate_limit_bps,omitempty"`

	// // SampleFrequency is an optional frequency for sampling how often
	// // acknowledgements are sampled for observability. See
	// // https://docs.nats.io/running-a-nats-service/nats_admin/monitoring/monitoring_jetstream
	// SampleFrequency string `json:"sample_freq,omitempty"`

	// // MaxWaiting is a maximum number of pull requests waiting to be
	// // fulfilled. If not set, this will inherit settings from stream's
	// // ConsumerLimits or (if those are not set) from account settings.  If
	// // neither are set, server default is 512.
	// MaxWaiting int `json:"max_waiting,omitempty"`

	// // MaxAckPending is a maximum number of outstanding unacknowledged
	// // messages. Once this limit is reached, the server will suspend sending
	// // messages to the consumer. If not set, server default is 1000.
	// // Set to -1 for unlimited.
	// MaxAckPending int `json:"max_ack_pending,omitempty"`

	// // HeadersOnly indicates whether only headers of messages should be sent
	// // (and no payload). Defaults to false.
	// HeadersOnly bool `json:"headers_only,omitempty"`

	// // MaxRequestBatch is the optional maximum batch size a single pull
	// // request can make. When set with MaxRequestMaxBytes, the batch size
	// // will be constrained by whichever limit is hit first.
	// MaxRequestBatch int `json:"max_batch,omitempty"`

	// // MaxRequestExpires is the maximum duration a single pull request will
	// // wait for messages to be available to pull.
	// MaxRequestExpires time.Duration `json:"max_expires,omitempty"`

	// // MaxRequestMaxBytes is the optional maximum total bytes that can be
	// // requested in a given batch. When set with MaxRequestBatch, the batch
	// // size will be constrained by whichever limit is hit first.
	// MaxRequestMaxBytes int `json:"max_bytes,omitempty"`

	// // InactiveThreshold is a duration which instructs the server to clean
	// // up the consumer if it has been inactive for the specified duration.
	// // Durable consumers will not be cleaned up by default, but if
	// // InactiveThreshold is set, they will be. If not set, this will inherit
	// // settings from stream's ConsumerLimits. If neither are set, server
	// // default is 5 seconds.
	// //
	// // A consumer is considered inactive there are not pull requests
	// // received by the server (for pull consumers), or no interest detected
	// // on deliver subject (for push consumers), not if there are no
	// // messages to be delivered.
	// InactiveThreshold time.Duration `json:"inactive_threshold,omitempty"`

	// // Replicas the number of replicas for the consumer's state. By default,
	// // consumers inherit the number of replicas from the stream.
	// Replicas int `json:"num_replicas"`

	// // MemoryStorage is a flag to force the consumer to use memory storage
	// // rather than inherit the storage type from the stream.
	// MemoryStorage bool `json:"mem_storage,omitempty"`

	// // FilterSubjects allows filtering messages from a stream by subject.
	// // This field is exclusive with FilterSubject. Requires nats-server
	// // v2.10.0 or later.
	// FilterSubjects []string `json:"filter_subjects,omitempty"`

	// // Metadata is a set of application-defined key-value pairs for
	// // associating metadata on the consumer. This feature requires
	// // nats-server v2.10.0 or later.
	// Metadata map[string]string `json:"metadata,omitempty"`

}

// InitConsumerConfig sets or updates the fields in a jetstream.ConsumerConfig
// based on the slip arguments provided.
func InitConsumerConfig(config *jetstream.ConsumerConfig, args slip.List) {
	for i := 0; i < len(args)-1; i += 2 {
		sym := args[i].(slip.Symbol)
		key := strings.ToLower(string(sym))
		if so := consumerOptMap[key]; so != nil {
			so.update(config, args[i+1])
		} else if key != ":timeout" {
			slip.NewPanic("%s is not a valid keyword", key)
		}
	}
}
