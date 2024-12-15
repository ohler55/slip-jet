// Copyright (c) 2024, Peter Ohler, All rights reserved.

package jet

import (
	"sort"
	"strings"
	"time"

	"github.com/nats-io/nats.go/jetstream"
	"github.com/ohler55/slip"
)

type orderedOpt struct {
	doc    *slip.DocArg
	update func(config *jetstream.OrderedConsumerConfig, v slip.Object)
}

var orderedOptMap = map[string]*orderedOpt{
	":filter-subjects": {
		doc: &slip.DocArg{
			Name: "filter-subjects",
			Type: "list",
			Text: `Allows filtering messages from a stream by subject. This field is
exclusive with FilterSubject. Requires nats-server v2.10.0 or later.`,
		},
		update: func(config *jetstream.OrderedConsumerConfig, v slip.Object) {
			list, ok := v.(slip.List)
			if !ok {
				slip.PanicType(":filter-subjects", v, "list")
			}
			for _, x := range list {
				config.FilterSubjects = append(config.FilterSubjects, slip.MustBeString(x, ":filter-subject"))
			}
		},
	},
	":deliver-policy": {
		doc: &slip.DocArg{
			Name: "deliver-policy",
			Type: ":all|:last|:new|:start-sequence|:start-time|:last-per-subject",
			Text: `Defines from which point to start delivering messages
from the stream. Defaults to _:all_.`,
		},
		update: func(config *jetstream.OrderedConsumerConfig, v slip.Object) {
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
				slip.PanicType(":deliver-policy", v,
					":all", ":last", ":new", ":start-sequence", ":start-time", ":last-per-subject")
			}
		},
	},
	":opt-start-seq": {
		doc: &slip.DocArg{
			Name: "opt-start-seq",
			Type: "fixnum",
			Text: `An optional sequence number from which to start
 message delivery. Only applicable when _:deliver-policy_ is set to
_:start-sequence_.`,
		},
		update: func(config *jetstream.OrderedConsumerConfig, v slip.Object) {
			if num, ok := v.(slip.Fixnum); ok {
				config.OptStartSeq = uint64(num)
			} else {
				slip.PanicType(":opt-start-seq", v, "fixnum")
			}
		},
	},
	":opt-start-time": {
		doc: &slip.DocArg{
			Name: "opt-start-time",
			Type: "time",
			Text: `An optional time from which to start message
delivery. Only applicable when _:deliver-policy_ is set to
_:start-time_.`,
		},
		update: func(config *jetstream.OrderedConsumerConfig, v slip.Object) {
			if stm, ok := v.(slip.Time); ok {
				tm := time.Time(stm)
				config.OptStartTime = &tm
			} else {
				slip.PanicType(":opt-start-time", v, "time")
			}
		},
	},
	":replay-policy": {
		doc: &slip.DocArg{
			Name: "replay-policy",
			Type: ":instant|:original",
			Text: `Defines the rate at which messages are sent to the consumer. If
_:replay-original-policy_ is set, messages are sent in the same intervals in which
they were stored on stream. This can be used e.g. to simulate production traffic in
development environments. If _:instant_ is set, messages are sent as fast as possible.
Defaults to _:instant_.`,
		},
		update: func(config *jetstream.OrderedConsumerConfig, v slip.Object) {
			switch v {
			case slip.Symbol(":instant"):
				config.ReplayPolicy = jetstream.ReplayInstantPolicy
			case slip.Symbol(":original"):
				config.ReplayPolicy = jetstream.ReplayOriginalPolicy
			default:
				slip.PanicType(":replay-policy", v, ":instant", ":original")
			}
		},
	},
	":inactive-threshold": {
		doc: &slip.DocArg{
			Name: "inactive-threshold",
			Type: "real",
			Text: `The duration which instructs the server to clean up the consumer
if it has been inactive for the specified duration. Durable consumers will not be
cleaned up by default, but if _:inactive-threshold_ is set, they will be. If not
set, this will inherit settings from stream's ConsumerLimits. If neither are set,
server default is 5 seconds. A consumer is considered inactive there are not pull
requests received by the server (for pull consumers), or no interest detected on
deliver subject (for push consumers), not if there are no messages to be delivered.`,
		},
		update: func(config *jetstream.OrderedConsumerConfig, v slip.Object) {
			if num, ok := v.(slip.Real); ok {
				config.InactiveThreshold = time.Duration(float64(time.Second) * num.RealValue())
			} else {
				slip.PanicType(":inactive-threshold", v, "real")
			}
		},
	},
	":headers-only": {
		doc: &slip.DocArg{
			Name: "headers-only",
			Type: "boolean",
			Text: `Indicates whether only headers of messages should be sent
(with no payload). Defaults to false.`,
		},
		update: func(config *jetstream.OrderedConsumerConfig, v slip.Object) {
			config.HeadersOnly = v != nil
		},
	},
	":max-reset-attempts": {
		doc: &slip.DocArg{
			Name: "max-reset-attempts",
			Type: "fixnum",
			Text: `Defines the number of attempts for the consumer to be recreated in a
single recreation cycle. Defaults to unlimited.`,
		},
		update: func(config *jetstream.OrderedConsumerConfig, v slip.Object) {
			if num, ok := v.(slip.Fixnum); ok {
				config.MaxResetAttempts = int(num)
			} else {
				slip.PanicType(":max-reset-attempts", v, "fixnum")
			}
		},
	},
}

// InitOrderedConfig sets or updates the fields in a
// jetstream.OrderedConsumerConfig based on the slip arguments provided.
func InitOrderedConfig(config *jetstream.OrderedConsumerConfig, args slip.List) {
	for i := 0; i < len(args)-1; i += 2 {
		sym := args[i].(slip.Symbol)
		key := strings.ToLower(string(sym))
		if so := orderedOptMap[key]; so != nil {
			so.update(config, args[i+1])
		} else if key != ":timeout" {
			slip.NewPanic("%s is not a valid keyword", key)
		}
	}
}

// OrderedConfigPropList returns a property list built from a
// jetstream.OrderedConsumerConfig. The returned list is suitable as arguments
// to an ordered consumer creation.
func OrderedConfigPropList(config *jetstream.OrderedConsumerConfig) slip.List {
	var (
		startTime   slip.Object
		headersOnly slip.Object
		filters     slip.List
	)
	if config.OptStartTime != nil {
		startTime = slip.Time(*config.OptStartTime)
	}
	if config.HeadersOnly {
		headersOnly = slip.True
	}
	for _, f := range config.FilterSubjects {
		filters = append(filters, slip.String(f))
	}
	return slip.List{
		slip.Symbol(":filter-subjects"), filters,
		slip.Symbol(":deliver-policy"), slip.Symbol(delPolMap[config.DeliverPolicy]),
		slip.Symbol(":opt-start-seq"), slip.Fixnum(config.OptStartSeq),
		slip.Symbol(":opt-start-time"), startTime,
		slip.Symbol(":replay-policy"), slip.Symbol(replayPolMap[config.ReplayPolicy]),
		slip.Symbol(":inactive-threshold"), slip.DoubleFloat(float64(config.InactiveThreshold) / float64(time.Second)),
		slip.Symbol(":headers-only"), headersOnly,
		slip.Symbol(":max-reset-attempts"), slip.Fixnum(config.MaxResetAttempts),
	}
}

func makeOrderedMethodDoc(method, args, retType, argDocs, description string) string {
	var b []byte
	b = append(b, "__"...)
	b = append(b, method...)
	b = append(b, "__ "...)
	b = append(b, args...)
	b = append(b, "&key _timeout_"...)
	keys := make([]string, 0, len(streamOptMap))
	for k := range orderedOptMap {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		b = append(b, ' ')
		b = append(b, k[1:]...)
	}
	b = append(b, " => "...)
	b = append(b, retType...)
	b = append(b, '\n')

	b = append(b, argDocs...)
	b = append(b, "   _:timeout_ [real] the number of seconds to wait before timing out."...)
	for _, k := range keys {
		b = append(b, "\n   _"...)
		b = append(b, k...)
		b = append(b, "_ ["...)
		doc := orderedOptMap[k].doc
		b = append(b, doc.Type...)
		b = append(b, "] "...)
		b = append(b, doc.Text...)
	}
	b = append(b, '\n', '\n', '\n')
	b = append(b, description...)
	b = append(b, '\n')

	return string(b)
}
