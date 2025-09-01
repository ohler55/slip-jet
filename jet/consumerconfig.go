// Copyright (c) 2024, Peter Ohler, All rights reserved.

package jet

import (
	"sort"
	"strings"
	"time"

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
			Name: ":name",
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
			Name: ":durable",
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
	":description": {
		doc: &slip.DocArg{
			Name: ":description",
			Type: "string",
			Text: `An optional description of the consumer.`,
		},
		update: func(config *jetstream.ConsumerConfig, v slip.Object) {
			config.Description = slip.MustBeString(v, ":description")
		},
	},
	":deliver-policy": {
		doc: &slip.DocArg{
			Name: ":deliver-policy",
			Type: ":all|:last|:new|:start-sequence|:start-time|:last-per-subject",
			Text: `Defines from which point to start delivering messages
from the stream. Defaults to _:all_.`,
		},
		update: func(config *jetstream.ConsumerConfig, v slip.Object) {
			polMap := map[string]jetstream.DeliverPolicy{
				":all":              jetstream.DeliverAllPolicy,
				":last":             jetstream.DeliverLastPolicy,
				":new":              jetstream.DeliverNewPolicy,
				":start-sequence":   jetstream.DeliverByStartSequencePolicy,
				":start-time":       jetstream.DeliverByStartTimePolicy,
				":last-per-subject": jetstream.DeliverLastPerSubjectPolicy,
			}
			sym, _ := v.(slip.Symbol)
			if pol, has := polMap[strings.ToLower(string(sym))]; has {
				config.DeliverPolicy = pol
			} else {
				slip.TypePanic(slip.NewScope(), 0, ":deliver-policy", v,
					":all", ":last", ":new", ":start-sequence", ":start-time", ":last-per-subject")
			}
		},
	},
	":opt-start-seq": {
		doc: &slip.DocArg{
			Name: ":opt-start-seq",
			Type: "fixnum",
			Text: `An optional sequence number from which to start
 message delivery. Only applicable when _:deliver-policy_ is set to
_:start-sequence_.`,
		},
		update: func(config *jetstream.ConsumerConfig, v slip.Object) {
			if num, ok := v.(slip.Fixnum); ok {
				config.OptStartSeq = uint64(num)
			} else {
				slip.TypePanic(slip.NewScope(), 0, ":opt-start-seq", v, "fixnum")
			}
		},
	},
	":opt-start-time": {
		doc: &slip.DocArg{
			Name: ":opt-start-time",
			Type: "time",
			Text: `An optional time from which to start message
delivery. Only applicable when _:deliver-policy_ is set to
_:start-time_.`,
		},
		update: func(config *jetstream.ConsumerConfig, v slip.Object) {
			if stm, ok := v.(slip.Time); ok {
				tm := time.Time(stm)
				config.OptStartTime = &tm
			} else {
				slip.TypePanic(slip.NewScope(), 0, ":opt-start-time", v, "time")
			}
		},
	},
	":ack-policy": {
		doc: &slip.DocArg{
			Name: ":ack-policy",
			Type: ":explicit|:all|:none",
			Text: `The acknowledgement policy for the consumer. Defaults to _:explicit_.`,
		},
		update: func(config *jetstream.ConsumerConfig, v slip.Object) {
			ackMap := map[string]jetstream.AckPolicy{
				":explicit": jetstream.AckExplicitPolicy,
				":all":      jetstream.AckAllPolicy,
				":none":     jetstream.AckNonePolicy,
			}
			sym, _ := v.(slip.Symbol)
			if pol, has := ackMap[strings.ToLower(string(sym))]; has {
				config.AckPolicy = pol
			} else {
				slip.TypePanic(slip.NewScope(), 0, ":ack-policy", v, ":explicit", ":all", ":none")
			}
		},
	},
	":ack-wait": {
		doc: &slip.DocArg{
			Name: ":ack-wait",
			Type: "real",
			Text: `Defines how long in seconds the server will wait for an acknowledgement
before resending a message. If not set, server default is 30 seconds.`,
		},
		update: func(config *jetstream.ConsumerConfig, v slip.Object) {
			if num, ok := v.(slip.Real); ok {
				config.AckWait = time.Duration(float64(time.Second) * num.RealValue())
			} else {
				slip.TypePanic(slip.NewScope(), 0, ":ack-wait", v, "real")
			}
		},
	},
	":max-deliver": {
		doc: &slip.DocArg{
			Name: ":max-deliver",
			Type: "fixnum",
			Text: `Defines the maximum number of delivery attempts for a message.
Applies to any message that is re-sent due to ack policy. If not set, server
default is -1 (unlimited).`,
		},
		update: func(config *jetstream.ConsumerConfig, v slip.Object) {
			if num, ok := v.(slip.Fixnum); ok {
				config.MaxDeliver = int(num)
			} else {
				slip.TypePanic(slip.NewScope(), 0, ":max-deliver", v, "fixnum")
			}
		},
	},
	":back-off": {
		doc: &slip.DocArg{
			Name: ":back-off",
			Type: "list of real",
			Text: `Specifies the optional back-off intervals for retrying
message delivery after a failed acknowledgement. It overrides _:ack-wait_ option.
_:back-off_ only applies to messages not acknowledged in the specified time,
not messages that were nak'ed. The number of intervals specified must be lower or
equal to _:max-deliver_. If the number of intervals is lower, the last interval is
used for all remaining attempts.`,
		},
		update: func(config *jetstream.ConsumerConfig, v slip.Object) {
			list, ok := v.(slip.List)
			if !ok {
				slip.TypePanic(slip.NewScope(), 0, ":back-off", v, "list of real")
			}
			var durs []time.Duration
			for _, x := range list {
				if num, ok := x.(slip.Real); ok {
					durs = append(durs, time.Duration(float64(time.Second)*num.RealValue()))
				} else {
					slip.TypePanic(slip.NewScope(), 0, ":back-off element", x, "real")
				}
			}
			config.BackOff = durs
		},
	},
	":filter-subject": {
		doc: &slip.DocArg{
			Name: ":filter-subject",
			Type: "string",
			Text: `Used to filter messages delivered from the stream. _:filter-subject_
is exclusive with _:filter-subjects_.`,
		},
		update: func(config *jetstream.ConsumerConfig, v slip.Object) {
			config.FilterSubject = slip.MustBeString(v, ":filter-subject")
		},
	},
	":replay-policy": {
		doc: &slip.DocArg{
			Name: ":replay-policy",
			Type: ":instant|:original",
			Text: `Defines the rate at which messages are sent to the consumer. If
_:replay-original-policy_ is set, messages are sent in the same intervals in which
they were stored on stream. This can be used e.g. to simulate production traffic in
development environments. If _:instant_ is set, messages are sent as fast as possible.
Defaults to _:instant_.`,
		},
		update: func(config *jetstream.ConsumerConfig, v slip.Object) {
			switch v {
			case slip.Symbol(":instant"):
				config.ReplayPolicy = jetstream.ReplayInstantPolicy
			case slip.Symbol(":original"):
				config.ReplayPolicy = jetstream.ReplayOriginalPolicy
			default:
				slip.TypePanic(slip.NewScope(), 0, ":replay-policy", v, ":instant", ":original")
			}
		},
	},
	":rate-limit": {
		doc: &slip.DocArg{
			Name: ":rate-limit",
			Type: "fixnum",
			Text: `An optional maximum rate of message delivery in bits per second.`,
		},
		update: func(config *jetstream.ConsumerConfig, v slip.Object) {
			if num, ok := v.(slip.Fixnum); ok {
				config.RateLimit = uint64(num)
			} else {
				slip.TypePanic(slip.NewScope(), 0, ":rate-limit", v, "fixnum")
			}
		},
	},
	":sample-frequency": {
		doc: &slip.DocArg{
			Name: ":sample-frequency",
			Type: "string",
			Text: `An optional frequency for sampling how often acknowledgements are
sampled for observability. See
https://docs.nats.io/running-a-nats-service/nats_admin/monitoring/monitoring_jetstream`,
		},
		update: func(config *jetstream.ConsumerConfig, v slip.Object) {
			config.SampleFrequency = slip.MustBeString(v, ":sample-frequency")
		},
	},
	":max-waiting": {
		doc: &slip.DocArg{
			Name: ":max-waiting",
			Type: "fixnum",
			Text: `A maximum number of pull requests waiting to be fulfilled. If not set,
this will inherit settings from stream's ConsumerLimits or (if those are not set) from
account settings. If neither are set, server default is 512.`,
		},
		update: func(config *jetstream.ConsumerConfig, v slip.Object) {
			if num, ok := v.(slip.Fixnum); ok {
				config.MaxWaiting = int(num)
			} else {
				slip.TypePanic(slip.NewScope(), 0, ":max-waiting", v, "fixnum")
			}
		},
	},
	":max-ack-pending": {
		doc: &slip.DocArg{
			Name: ":max-ack-pending",
			Type: "fixnum",
			Text: `A maximum number of outstanding unacknowledged messages. Once this
limit is reached, the server will suspend sending messages to the consumer. If not set,
server default is 1000. Set to -1 for unlimited.`,
		},
		update: func(config *jetstream.ConsumerConfig, v slip.Object) {
			if num, ok := v.(slip.Fixnum); ok {
				config.MaxAckPending = int(num)
			} else {
				slip.TypePanic(slip.NewScope(), 0, ":max-ack-pending", v, "fixnum")
			}
		},
	},
	":headers-only": {
		doc: &slip.DocArg{
			Name: ":headers-only",
			Type: "boolean",
			Text: `Indicates whether only headers of messages should be sent
(with no payload). Defaults to false.`,
		},
		update: func(config *jetstream.ConsumerConfig, v slip.Object) {
			config.HeadersOnly = v != nil
		},
	},
	":max-request-batch": {
		doc: &slip.DocArg{
			Name: ":max-request-batch",
			Type: "fixnum",
			Text: `The optional maximum batch size a single pull request can make.
When set with MaxRequestMaxBytes, the batch size will be constrained by whichever
limit is hit first.`,
		},
		update: func(config *jetstream.ConsumerConfig, v slip.Object) {
			if num, ok := v.(slip.Fixnum); ok {
				config.MaxRequestBatch = int(num)
			} else {
				slip.TypePanic(slip.NewScope(), 0, ":max-request-batch", v, "fixnum")
			}
		},
	},
	":max-request-expires": {
		doc: &slip.DocArg{
			Name: ":max-request-expires",
			Type: "real",
			Text: `The maximum duration a single pull request will
wait for messages to be available to pull.`,
		},
		update: func(config *jetstream.ConsumerConfig, v slip.Object) {
			if num, ok := v.(slip.Real); ok {
				config.MaxRequestExpires = time.Duration(float64(time.Second) * num.RealValue())
			} else {
				slip.TypePanic(slip.NewScope(), 0, ":max-request-expires", v, "real")
			}
		},
	},
	":max-request-max-bytes": {
		doc: &slip.DocArg{
			Name: ":max-request-max-bytes",
			Type: "fixnum",
			Text: `The optional maximum total bytes that can be requested in a given
batch. When set with MaxRequestBatch, the batch size will be constrained by whichever
limit is hit first.`,
		},
		update: func(config *jetstream.ConsumerConfig, v slip.Object) {
			if num, ok := v.(slip.Fixnum); ok {
				config.MaxRequestMaxBytes = int(num)
			} else {
				slip.TypePanic(slip.NewScope(), 0, ":max-request-max-bytes", v, "fixnum")
			}
		},
	},
	":inactive-threshold": {
		doc: &slip.DocArg{
			Name: ":inactive-threshold",
			Type: "real",
			Text: `The duration which instructs the server to clean up the consumer
if it has been inactive for the specified duration. Durable consumers will not be
cleaned up by default, but if _:inactive-threshold_ is set, they will be. If not
set, this will inherit settings from stream's ConsumerLimits. If neither are set,
server default is 5 seconds. A consumer is considered inactive there are not pull
requests received by the server (for pull consumers), or no interest detected on
deliver subject (for push consumers), not if there are no messages to be delivered.`,
		},
		update: func(config *jetstream.ConsumerConfig, v slip.Object) {
			if num, ok := v.(slip.Real); ok {
				config.InactiveThreshold = time.Duration(float64(time.Second) * num.RealValue())
			} else {
				slip.TypePanic(slip.NewScope(), 0, ":inactive-threshold", v, "real")
			}
		},
	},
	":replicas": {
		doc: &slip.DocArg{
			Name: ":replicas",
			Type: "fixnum",
			Text: `The number of replicas for the consumer's state. By default,
consumers inherit the number of replicas from the stream.`,
		},
		update: func(config *jetstream.ConsumerConfig, v slip.Object) {
			if num, ok := v.(slip.Fixnum); ok {
				config.Replicas = int(num)
			} else {
				slip.TypePanic(slip.NewScope(), 0, ":replicas", v, "fixnum")
			}
		},
	},
	":memory-storage": {
		doc: &slip.DocArg{
			Name: ":memory-storage",
			Type: "boolean",
			Text: `A flag to force the consumer to use memory storage
rather than inherit the storage type from the stream.`,
		},
		update: func(config *jetstream.ConsumerConfig, v slip.Object) {
			config.MemoryStorage = v != nil
		},
	},
	":filter-subjects": {
		doc: &slip.DocArg{
			Name: ":filter-subjects",
			Type: "list",
			Text: `Allows filtering messages from a stream by subject. This field is
exclusive with FilterSubject. Requires nats-server v2.10.0 or later.`,
		},
		update: func(config *jetstream.ConsumerConfig, v slip.Object) {
			list, ok := v.(slip.List)
			if !ok {
				slip.TypePanic(slip.NewScope(), 0, ":filter-subjects", v, "list")
			}
			for _, x := range list {
				config.FilterSubjects = append(config.FilterSubjects, slip.MustBeString(x, ":filter-subject"))
			}
		},
	},
	":metadata": {
		doc: &slip.DocArg{
			Name: ":metadata",
			Type: "property list",
			Text: `A set of application-defined key-value pairs for associating metadata
on the consumer. This feature requires nats-server v2.10.0 or later.`,
		},
		update: func(config *jetstream.ConsumerConfig, v slip.Object) {
			plist, ok := v.(slip.List)
			if !ok {
				slip.TypePanic(slip.NewScope(), 0, ":metadata", v, "property list")
			}
			m := map[string]string{}
			for i := 0; i < len(plist)-1; i += 2 {
				key := slip.MustBeString(plist[i], ":metadata key")
				m[key] = slip.MustBeString(plist[i+1], ":metadata value")
			}
			config.Metadata = m
		},
	},
	":pause-until": {
		doc: &slip.DocArg{
			Name: ":pause-until",
			Type: "time",
			Text: `For suspending the consumer until the deadline.`,
		},
		update: func(config *jetstream.ConsumerConfig, v slip.Object) {
			if stm, ok := v.(slip.Time); ok {
				tm := time.Time(stm)
				config.PauseUntil = &tm
			} else {
				slip.TypePanic(slip.NewScope(), 0, ":pause-until", v, "time")
			}
		},
	},
	":priority-policy": {
		doc: &slip.DocArg{
			Name: ":priority-policy",
			Type: "nil|:none|:pinned|:overflow",
			Text: `Represents he priority policy the consumer is set to.
If _:none_ or _nil_ is the default priority policy.
If _:pinned_ is the priority policy that pins a consumer to a specific client.
If _:overflow_ is the priority policy that allows for restricting when a
consumer will receive messages based on the number of pending messages or acks.
`,
		},
		update: func(config *jetstream.ConsumerConfig, v slip.Object) {
			switch v {
			case slip.Symbol(":nonde"), nil:
				config.PriorityPolicy = jetstream.PriorityPolicyNone
			case slip.Symbol(":pinned"):
				config.PriorityPolicy = jetstream.PriorityPolicyPinned
			case slip.Symbol(":overflow"):
				config.PriorityPolicy = jetstream.PriorityPolicyOverflow
			default:
				slip.TypePanic(slip.NewScope(), 0, ":priority-policy", v, "nil", ":none", ":pinned", ":overflow")
			}
		},
	},
	":pinned-ttl": {
		doc: &slip.DocArg{
			Name: ":pinned-ttl",
			Type: "real",
			Text: `Represents the time after which the client will be unpinned
if no new pull requests are sent.Used with PriorityPolicyPinned.`,
		},
		update: func(config *jetstream.ConsumerConfig, v slip.Object) {
			if num, ok := v.(slip.Real); ok {
				config.PinnedTTL = time.Duration(float64(time.Second) * num.RealValue())
			} else {
				slip.TypePanic(slip.NewScope(), 0, ":pinned-ttl", v, "real")
			}
		},
	},
	":priority-groups": {
		doc: &slip.DocArg{
			Name: ":priority-groups",
			Type: "list",
			Text: `A list of priority groups this consumer supports.`,
		},
		update: func(config *jetstream.ConsumerConfig, v slip.Object) {
			list, ok := v.(slip.List)
			if !ok {
				slip.TypePanic(slip.NewScope(), 0, ":priority-groups", v, "list")
			}
			for _, x := range list {
				config.PriorityGroups = append(config.PriorityGroups, slip.MustBeString(x, ":priority-groups"))
			}
		},
	},
	":deliver-subject": {
		doc: &slip.DocArg{
			Name: ":deliver-subject",
			Type: "string",
			Text: `The subject to deliver messages to for push consumers.`,
		},
		update: func(config *jetstream.ConsumerConfig, v slip.Object) {
			config.DeliverSubject = slip.MustBeString(v, ":deliver-subject")
		},
	},
	":deliver-group": {
		doc: &slip.DocArg{
			Name: ":deliver-group",
			Type: "string",
			Text: `The group name for push consumers.`,
		},
		update: func(config *jetstream.ConsumerConfig, v slip.Object) {
			config.DeliverGroup = slip.MustBeString(v, ":deliver-group")
		},
	},
	":flow-control": {
		doc: &slip.DocArg{
			Name: ":flow-control",
			Type: "boolean",
			Text: `A flag to enable flow control for the consumer.
When set, server will regularly send an empty message with Status
header 100 and a reply subject, consumers must reply to these
messages to control the rate of message delivery.`,
		},
		update: func(config *jetstream.ConsumerConfig, v slip.Object) {
			config.FlowControl = v != nil
		},
	},
	":idle-heartbeat": {
		doc: &slip.DocArg{
			Name: ":idle-heartbeat",
			Type: "real",
			Text: `Enables push consumer idle heartbeat messages.
If the Consumer is idle for more than the set value, an empty message
with Status header 100 will be sent indicating the consumer is still
alive.`,
		},
		update: func(config *jetstream.ConsumerConfig, v slip.Object) {
			if num, ok := v.(slip.Real); ok {
				config.IdleHeartbeat = time.Duration(float64(time.Second) * num.RealValue())
			} else {
				slip.TypePanic(slip.NewScope(), 0, ":idle-heartbeat", v, "real")
			}
		},
	},
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
			slip.ErrorPanic(slip.NewScope(), 0, "%s is not a valid keyword", key)
		}
	}
}

var (
	delPolMap = map[jetstream.DeliverPolicy]string{
		jetstream.DeliverAllPolicy:             ":all",
		jetstream.DeliverLastPolicy:            ":last",
		jetstream.DeliverNewPolicy:             ":new",
		jetstream.DeliverByStartSequencePolicy: ":start-sequence",
		jetstream.DeliverByStartTimePolicy:     ":start-time",
		jetstream.DeliverLastPerSubjectPolicy:  ":last-per-subject",
	}
	ackPolMap = map[jetstream.AckPolicy]string{
		jetstream.AckExplicitPolicy: ":explicit",
		jetstream.AckAllPolicy:      ":all",
		jetstream.AckNonePolicy:     ":none",
	}
	replayPolMap = map[jetstream.ReplayPolicy]string{
		jetstream.ReplayInstantPolicy:  ":instant",
		jetstream.ReplayOriginalPolicy: ":original",
	}
	priorityPolMap = map[jetstream.PriorityPolicy]string{
		jetstream.PriorityPolicyNone:     ":none",
		jetstream.PriorityPolicyPinned:   ":pinned",
		jetstream.PriorityPolicyOverflow: ":overflow",
	}
)

// ConsumerConfigPropList returns a property list built from a
// jetstream.ConsumerConfig. The returned list is suitable as arguments to a
// consumer creation.
func ConsumerConfigPropList(config *jetstream.ConsumerConfig) slip.List {
	var (
		startTime      slip.Object
		backoff        slip.List
		headersOnly    slip.Object
		memStore       slip.Object
		filters        slip.List
		meta           slip.List
		pauseUntil     slip.Object
		priorityGroups slip.List
		flowControl    slip.Object
	)
	if config.OptStartTime != nil {
		startTime = slip.Time(*config.OptStartTime)
	}
	for _, dur := range config.BackOff {
		backoff = append(backoff, slip.DoubleFloat(float64(dur)/float64(time.Second)))
	}
	if config.HeadersOnly {
		headersOnly = slip.True
	}
	if config.MemoryStorage {
		memStore = slip.True
	}
	for _, f := range config.FilterSubjects {
		filters = append(filters, slip.String(f))
	}
	for k, v := range config.Metadata {
		meta = append(meta, slip.String(k), slip.String(v))
	}
	if config.PauseUntil != nil {
		pauseUntil = slip.Time(*config.PauseUntil)
	}
	for _, pg := range config.PriorityGroups {
		priorityGroups = append(priorityGroups, slip.String(pg))
	}
	if config.FlowControl {
		flowControl = slip.True
	}
	return slip.List{
		slip.Symbol(":name"), slip.String(config.Name),
		slip.Symbol(":durable"), slip.String(config.Durable),
		slip.Symbol(":description"), slip.String(config.Description),
		slip.Symbol(":deliver-policy"), slip.Symbol(delPolMap[config.DeliverPolicy]),
		slip.Symbol(":opt-start-seq"), slip.Fixnum(config.OptStartSeq),
		slip.Symbol(":opt-start-time"), startTime,
		slip.Symbol(":ack-policy"), slip.Symbol(ackPolMap[config.AckPolicy]),
		slip.Symbol(":ack-wait"), slip.DoubleFloat(float64(config.AckWait) / float64(time.Second)),
		slip.Symbol(":max-deliver"), slip.Fixnum(config.MaxDeliver),
		slip.Symbol(":back-off"), backoff,
		slip.Symbol(":filter-subject"), slip.String(config.FilterSubject),
		slip.Symbol(":replay-policy"), slip.Symbol(replayPolMap[config.ReplayPolicy]),
		slip.Symbol(":rate-limit"), slip.Fixnum(config.RateLimit),
		slip.Symbol(":sample-frequency"), slip.String(config.SampleFrequency),
		slip.Symbol(":max-waiting"), slip.Fixnum(config.MaxWaiting),
		slip.Symbol(":max-ack-pending"), slip.Fixnum(config.MaxAckPending),
		slip.Symbol(":headers-only"), headersOnly,
		slip.Symbol(":max-request-batch"), slip.Fixnum(config.MaxRequestBatch),
		slip.Symbol(":max-request-expires"), slip.DoubleFloat(float64(config.MaxRequestExpires) / float64(time.Second)),
		slip.Symbol(":max-request-max-bytes"), slip.Fixnum(config.MaxRequestMaxBytes),
		slip.Symbol(":inactive-threshold"), slip.DoubleFloat(float64(config.InactiveThreshold) / float64(time.Second)),
		slip.Symbol(":replicas"), slip.Fixnum(config.Replicas),
		slip.Symbol(":memory-storage"), memStore,
		slip.Symbol(":filter-subjects"), filters,
		slip.Symbol(":metadata"), meta,
		slip.Symbol(":pause-until"), pauseUntil,
		slip.Symbol(":priority-policy"), slip.Symbol(priorityPolMap[config.PriorityPolicy]),
		slip.Symbol(":pinned-ttl"), slip.DoubleFloat(float64(config.PinnedTTL) / float64(time.Second)),
		slip.Symbol(":priority-groups"), priorityGroups,
		slip.Symbol(":deliver-subject"), slip.String(config.DeliverSubject),
		slip.Symbol(":deliver-group"), slip.String(config.DeliverGroup),
		slip.Symbol(":flow-control"), flowControl,
		slip.Symbol(":idle-heartbeat"), slip.DoubleFloat(float64(config.IdleHeartbeat) / float64(time.Second)),
	}
}

func makeConsumerMethodFuncDoc(method string, arg *slip.DocArg, retType, description string) *slip.FuncDoc {
	fd := slip.FuncDoc{
		Name:   method,
		Return: retType,
		Text:   description,
		Kind:   slip.MethodSymbol,
	}
	if arg != nil {
		fd.Args = append(fd.Args, arg)
	}
	fd.Args = append(fd.Args,
		&slip.DocArg{Name: "&key"},
		&slip.DocArg{Name: ":timeout", Type: "real", Text: "The number of seconds to wait before timing out."},
	)
	keys := make([]string, 0, len(consumerOptMap))
	for k := range consumerOptMap {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		doc := consumerOptMap[k].doc
		fd.Args = append(fd.Args, doc)
	}
	return &fd
}
