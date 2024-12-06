// Copyright (c) 2024, Peter Ohler, All rights reserved.

package jet

import (
	"sort"
	"strings"
	"time"

	"github.com/nats-io/nats.go/jetstream"
	"github.com/ohler55/slip"
	"github.com/ohler55/slip/pkg/flavors"
)

type streamOpt struct {
	doc    *slip.DocArg
	update func(config *jetstream.StreamConfig, v slip.Object)
}

var streamOptMap = map[string]*streamOpt{
	":name": {
		doc: &slip.DocArg{
			Name: "name",
			Type: "string",
			Text: `Name is the name of the stream. It is required and must be
unique across the JetStream account. Names cannot contain whitespace, ., *, >,
path separators (forward or backwards slash), and non-printable characters.`,
		},
		update: func(config *jetstream.StreamConfig, v slip.Object) {
			config.Name = slip.MustBeString(v, ":name")
		},
	},
	":allow-direct": {
		doc: &slip.DocArg{
			Name: "allow-direct",
			Type: "boolean",
			Text: `Enables direct access to individual messages using direct get API. Defaults to _nil_.`,
		},
		update: func(config *jetstream.StreamConfig, v slip.Object) {
			config.AllowDirect = (v != nil)
		},
	},
	":allow-rollup": {
		doc: &slip.DocArg{
			Name: "allow-rollup",
			Type: "boolean",
			Text: `Allows the use of the Nats-Rollup header to replace all
contents of a stream, or subject in a stream, with a single new message.`,
		},
		update: func(config *jetstream.StreamConfig, v slip.Object) {
			config.AllowRollup = (v != nil)
		},
	},
	":compression": {
		doc: &slip.DocArg{
			Name: "compression",
			Type: "bool",
			Text: `Specifies the message storage compression algorithm. Defaults to NoCompression.`,
		},
		update: func(config *jetstream.StreamConfig, v slip.Object) {
			if v == nil {
				config.Compression = jetstream.NoCompression
			} else {
				config.Compression = jetstream.S2Compression
			}
		},
	},
	":consumer-limits": {
		doc: &slip.DocArg{
			Name: "consumer-limits",
			Type: "list",
			Text: `Defines limits of certain values that consumers can set, defaults for those
who don't set these settings. The value expected is a list of two
elements. The first is the inactive-threshold which is the number of seconds
as a _real_. The second element is the max-ack-pending which must be a
_fixnum_. The inactive-threshold is a duration which instructs the server to
clean up the consumer if it has been inactive for the specified
duration. While the max-ack-pending a maximum number of outstanding
unacknowledged messages for a consumer.`,
		},
		update: func(config *jetstream.StreamConfig, v slip.Object) {
			list, ok := v.(slip.List)
			if !ok || len(list) != 2 {
				slip.PanicType(":consumer-limits", v, "list of a real and a fixnum")
			}
			if num, ok2 := list[0].(slip.Real); ok2 {
				config.ConsumerLimits.InactiveThreshold = time.Duration(float64(time.Second) * num.RealValue())
			} else {
				slip.PanicType(":consumer-limits", v, "list of a real and a fixnum")
			}
			if num, ok2 := list[1].(slip.Fixnum); ok2 {
				config.ConsumerLimits.MaxAckPending = int(num)
			} else {
				slip.PanicType(":consumer-limits", v, "list of a real and a fixnum")
			}
		},
	},
	":deny-delete": {
		doc: &slip.DocArg{
			Name: "deny-delete",
			Type: "boolean",
			Text: `Restricts the ability to delete messages from a stream via  the API. Defaults to false.`,
		},
		update: func(config *jetstream.StreamConfig, v slip.Object) {
			config.DenyDelete = (v != nil)
		},
	},
	":deny-purge": {
		doc: &slip.DocArg{
			Name: "deny-purge",
			Type: "boolean",
			Text: `Restricts the ability to purge messages from a stream via the API. Defaults to false.`,
		},
		update: func(config *jetstream.StreamConfig, v slip.Object) {
			config.DenyPurge = (v != nil)
		},
	},
	":description": {
		doc: &slip.DocArg{
			Name: "description",
			Type: "string",
			Text: `An optional description of the stream.`,
		},
		update: func(config *jetstream.StreamConfig, v slip.Object) {
			config.Description = slip.MustBeString(v, ":description")
		},
	},
	":discard": {
		doc: &slip.DocArg{
			Name: "discard",
			Type: ":old|:new",
			Text: `Defines the policy for handling messages when the stream reaches
its limits in terms of number of messages or total bytes. :old, the default, will
remove older messages to return to the limits. :new will fail to store new messages
once the limits are reached.`,
		},
		update: func(config *jetstream.StreamConfig, v slip.Object) {
			switch v {
			case slip.Symbol(":old"), nil:
				config.Discard = jetstream.DiscardOld
			case slip.Symbol(":new"):
				config.Discard = jetstream.DiscardNew
			default:
				slip.PanicType(":discard", v, "nil", ":old", ":new")
			}
		},
	},
	":discard-new-per-subject": {
		doc: &slip.DocArg{
			Name: "discard-new-per-subject",
			Type: "boolean",
			Text: `A flag to enable discarding new messages per subject when limits
are reached. Requires DiscardPolicy to be DiscardNew and the MaxMsgsPerSubject to be set.`,
		},
		update: func(config *jetstream.StreamConfig, v slip.Object) {
			config.DiscardNewPerSubject = (v != nil)
		},
	},
	":duplicates": {
		doc: &slip.DocArg{
			Name: "duplicates",
			Type: "real",
			Text: `Is the window within which to track duplicate messages.
If not set, server default is 2 minutes. The value must be a real and is
assumed to be seconds.`,
		},
		update: func(config *jetstream.StreamConfig, v slip.Object) {
			if num, ok := v.(slip.Real); ok {
				config.Duplicates = time.Duration(float64(time.Second) * num.RealValue())
			} else {
				slip.PanicType(":duplicates", v, "real")
			}
		},
	},
	":first-seq": {
		doc: &slip.DocArg{
			Name: "first-seq",
			Type: "fixnum",
			Text: `The initial sequence number of the first message in the stream.`,
		},
		update: func(config *jetstream.StreamConfig, v slip.Object) {
			if num, ok := v.(slip.Fixnum); ok {
				config.FirstSeq = uint64(num)
			} else {
				slip.PanicType(":first-seq", v, "fixnum")
			}
		},
	},
	":max-age": {
		doc: &slip.DocArg{
			Name: "max-age",
			Type: "real",
			Text: `The maximum age in seconds of messages that the stream will retain.`,
		},
		update: func(config *jetstream.StreamConfig, v slip.Object) {
			if num, ok := v.(slip.Real); ok {
				config.MaxAge = time.Duration(float64(time.Second) * num.RealValue())
			} else {
				slip.PanicType(":max-age", v, "real")
			}
		},
	},
	":max-bytes": {
		doc: &slip.DocArg{
			Name: "max-bytes",
			Type: "fixnum",
			Text: `The maximum total size of messages the stream will store.
After reaching the limit, stream adheres to the discard policy.
If not set, server default is -1 (unlimited).`,
		},
		update: func(config *jetstream.StreamConfig, v slip.Object) {
			if num, ok := v.(slip.Fixnum); ok {
				config.MaxBytes = int64(num)
			} else {
				slip.PanicType(":max-bytes", v, "fixnum")
			}
		},
	},
	":max-consumers": {
		doc: &slip.DocArg{
			Name: "max-consumers",
			Type: "fixnum",
			Text: `Specifies the maximum number of consumers allowed for the stream.`,
		},
		update: func(config *jetstream.StreamConfig, v slip.Object) {
			if num, ok := v.(slip.Fixnum); ok {
				config.MaxConsumers = int(num)
			} else {
				slip.PanicType(":max-consumers", v, "fixnum")
			}
		},
	},
	":max-msg-size": {
		doc: &slip.DocArg{
			Name: "max-msg-size",
			Type: "fixnum",
			Text: `The maximum size of any single message in the stream.`,
		},
		update: func(config *jetstream.StreamConfig, v slip.Object) {
			if num, ok := v.(slip.Fixnum); ok {
				config.MaxMsgSize = int32(num)
			} else {
				slip.PanicType(":max-msg-size", v, "fixnum")
			}
		},
	},
	":max-msgs": {
		doc: &slip.DocArg{
			Name: "max-msgs",
			Type: "fixnum",
			Text: `The maximum number of messages the stream will store.
After reaching the limit, stream adheres to the discard policy.
If not set, server default is -1 (unlimited).`,
		},
		update: func(config *jetstream.StreamConfig, v slip.Object) {
			if num, ok := v.(slip.Fixnum); ok {
				config.MaxMsgs = int64(num)
			} else {
				slip.PanicType(":max-msgs", v, "fixnum")
			}
		},
	},
	":max-msgs-per-subject": {
		doc: &slip.DocArg{
			Name: "max-msgs-per-subject",
			Type: "fixnum",
			Text: `The maximum number of messages per subject that the stream will retain.`,
		},
		update: func(config *jetstream.StreamConfig, v slip.Object) {
			if num, ok := v.(slip.Fixnum); ok {
				config.MaxMsgsPerSubject = int64(num)
			} else {
				slip.PanicType(":max-msgs-per-subject", v, "fixnum")
			}
		},
	},
	":metadata": {
		doc: &slip.DocArg{
			Name: "metadata",
			Type: "property list",
			Text: `A set of application-defined key-value pairs for
associating metadata on the stream. This feature requires nats-server
v2.10.0 or later.`,
		},
		update: func(config *jetstream.StreamConfig, v slip.Object) {
			plist, ok := v.(slip.List)
			if !ok {
				slip.PanicType(":metadata", v, "property list")
			}
			m := map[string]string{}
			for i := 0; i < len(plist)-1; i += 2 {
				key := slip.MustBeString(plist[i], ":metadata key")
				m[key] = slip.MustBeString(plist[i+1], ":metadata value")
			}
			config.Metadata = m
		},
	},
	":mirror": {
		doc: &slip.DocArg{
			Name: "mirror",
			Type: "jet-stream-source instance",
			Text: `Defines the configuration for mirroring another stream.`,
		},
		update: func(config *jetstream.StreamConfig, v slip.Object) {
			if inst, ok := v.(*flavors.Instance); ok && inst.IsA(streamSourceFlavor) {
				var jss jetstream.StreamSource
				SetJetstreamStreamSource(&jss, inst)
				config.Mirror = &jss
			} else if v != nil {
				slip.PanicType(":mirror", v, "jet-stream-source instance")
			}
		},
	},
	":mirror-direct": {
		doc: &slip.DocArg{
			Name: "mirror-direct",
			Type: "boolean",
			Text: `Enables direct access to individual messages from the
origin stream using direct get API. Defaults to _nil_.`,
		},
		update: func(config *jetstream.StreamConfig, v slip.Object) {
			config.MirrorDirect = (v != nil)
		},
	},
	":no-ack": {
		doc: &slip.DocArg{
			Name: "no-ack",
			Type: "boolean",
			Text: `A flag to disable acknowledging messages received by this stream.
If set to true, publish methods from the JetStream client will not
work as expected, since they rely on acknowledgements. Core NATS
publish methods should be used instead. Note that this will make
message delivery less reliable.`,
		},
		update: func(config *jetstream.StreamConfig, v slip.Object) {
			config.NoAck = (v != nil)
		},
	},
	":placement": {
		doc: &slip.DocArg{
			Name: "placement",
			Type: "list of strings (cluster tags...)",
			Text: `Used to declare where the stream should be placed via
tags and/or an explicit cluster name. The list of string starts with a cluster
name which is the name of the cluster to which the stream should be assigned.
The cluster name can be followed by zero or more tags which are used to match
streams to servers in the cluster. A stream will be assigned to a server with
a matching tag.`,
		},
		update: func(config *jetstream.StreamConfig, v slip.Object) {
			list, ok := v.(slip.List)
			if !ok || len(list) < 1 {
				slip.PanicType(":placement", v, "list")
			}
			p := jetstream.Placement{Cluster: slip.MustBeString(list[0], ":cluster")}
			for _, x := range list[1:] {
				p.Tags = append(p.Tags, slip.MustBeString(x, ":tag"))
			}
			config.Placement = &p
		},
	},
	":re-publish": {
		doc: &slip.DocArg{
			Name: "re-publish",
			Type: "list of (source destination headers-only)",
			Text: `Allows immediate republishing a message to the configured subject
after it's stored. The source is the subject pattern to match incoming messages against.
The destination is the subject pattern to republish the subject to. While the headers if
present is a flag to indicate that only the headers should be republished.`,
		},
		update: func(config *jetstream.StreamConfig, v slip.Object) {
			list, ok := v.(slip.List)
			if !ok || len(list) < 2 {
				slip.PanicType(":re-publish", v, "list of source, destination, and headers-only")
			}
			config.RePublish = &jetstream.RePublish{
				Source:      slip.MustBeString(list[0], ":re-publish source"),
				Destination: slip.MustBeString(list[1], ":re-publish destination"),
				HeadersOnly: (2 < len(list) && list[2] != nil),
			}
		},
	},
	":replicas": {
		doc: &slip.DocArg{
			Name: "replicas",
			Type: "fixnum",
			Text: `The number of stream replicas in clustered JetStream. Defaults to 1, maximum is 5.`,
		},
		update: func(config *jetstream.StreamConfig, v slip.Object) {
			if num, ok := v.(slip.Fixnum); ok {
				config.Replicas = int(num)
			} else {
				slip.PanicType(":replicas", v, "fixnum")
			}
		},
	},
	":retention": {
		doc: &slip.DocArg{
			Name: "retention",
			Type: ":limit|:interest|:queue]",
			Text: `Defines the message retention policy for the stream.
Defaults to LimitsPolicy. :limits (default) means that messages are retained until any given limit is
reached. This could be one of max-msgs, max-bytes, or max-age. :interest
specifies that when all known observables have acknowledged a message it can
be removed. :queue specifies that when the first worker or subscriber
acknowledges the message it can be removed.`,
		},
		update: func(config *jetstream.StreamConfig, v slip.Object) {
			switch v {
			case slip.Symbol(":limit"):
				config.Retention = jetstream.LimitsPolicy
			case slip.Symbol(":interest"):
				config.Retention = jetstream.InterestPolicy
			case slip.Symbol(":queue"):
				config.Retention = jetstream.WorkQueuePolicy
			default:
				slip.PanicType(":retention", v, ":limit", ":interest", ":queue")
			}
		},
	},
	":sealed": {
		doc: &slip.DocArg{
			Name: "sealed",
			Type: "boolean",
			Text: `Sealed streams do not allow messages to be published or deleted via limits or API,
sealed streams can not be unsealed via configuration update. Can only
be set on already created streams via the Update API.`,
		},
		update: func(config *jetstream.StreamConfig, v slip.Object) {
			config.Sealed = (v != nil)
		},
	},
	":sources": {
		doc: &slip.DocArg{
			Name: "sources",
			Type: "list of jet-stream-source instances",
			Text: `A list of other streams this stream sources messages from.`,
		},
		update: func(config *jetstream.StreamConfig, v slip.Object) {
			if v != nil {
				list, ok := v.(slip.List)
				if !ok {
					slip.PanicType(":sources", v, "list")
				}
				for _, x := range list {
					if inst, ok := x.(*flavors.Instance); ok && inst.IsA(streamSourceFlavor) {
						var jss jetstream.StreamSource
						SetJetstreamStreamSource(&jss, inst)
						config.Sources = append(config.Sources, &jss)
					} else {
						slip.PanicType(":sources", x, "jet-stream-source instance")
					}
				}
			}
		},
	},
	":storage": {
		doc: &slip.DocArg{
			Name: "storage",
			Type: ":file|:memory",
			Text: `Specifies the type of storage backend used for the stream as either
file or memory. :file specifies on disk storage. It's the default. :memory specifies in
memory only.`,
		},
		update: func(config *jetstream.StreamConfig, v slip.Object) {
			switch v {
			case slip.Symbol(":file"):
				config.Storage = jetstream.FileStorage
			case slip.Symbol(":memory"):
				config.Storage = jetstream.MemoryStorage
			default:
				slip.PanicType(":storage", v, ":file", ":memory")
			}
		},
	},
	":subject-transform": {
		doc: &slip.DocArg{
			Name: "subject-transform",
			Type: "list",
			Text: `Allows applying a transformation to matching messages' subjects. The
list must be a list of source and destination as strings. Source is the subject pattern
to match incoming messages against. Destination is the subject pattern to remap the subject to.`,
		},
		update: func(config *jetstream.StreamConfig, v slip.Object) {
			if v != nil {
				list, ok := v.(slip.List)
				if !ok || len(list) != 2 {
					slip.PanicType(":subject-transform", v, "list")
				}
				config.SubjectTransform = &jetstream.SubjectTransformConfig{
					Source:      slip.MustBeString(list[0], ":source"),
					Destination: slip.MustBeString(list[1], ":destination"),
				}
			}
		},
	},
	":subjects": {
		doc: &slip.DocArg{
			Name: "subjects",
			Type: "list",
			Text: `A list of subjects that the stream is listening on.
Wildcards are supported. Subjects cannot be set if the stream is
created as a mirror.`,
		},
		update: func(config *jetstream.StreamConfig, v slip.Object) {
			list, ok := v.(slip.List)
			if !ok {
				slip.PanicType(":subjects", v, "list")
			}
			for _, x := range list {
				config.Subjects = append(config.Subjects, slip.MustBeString(x, ":subject"))
			}
		},
	},
}

// InitStreamConfig sets or updates the fields in a jetstream.StreamConfig
// based on the slip arguments provided.
func InitStreamConfig(config *jetstream.StreamConfig, args slip.List) {
	for i := 0; i < len(args)-1; i += 2 {
		sym := args[i].(slip.Symbol)
		key := strings.ToLower(string(sym))
		if so := streamOptMap[key]; so != nil {
			so.update(config, args[i+1])
		} else if key != ":timeout" {
			slip.NewPanic("%s is not a valid keyword", key)
		}
	}
}

// StreamConfigPropList returns a property list built from a
// jetstream.StreamConfig. The returned list is suitable as arguments to a
// stream creation.
func StreamConfigPropList(config *jetstream.StreamConfig) slip.List {
	var (
		discard   slip.Object
		meta      slip.List
		mirror    slip.Object
		placement slip.List
		repub     slip.List
		retention slip.Object
		sources   slip.List
		storage   slip.Object
		transform slip.List
		subjects  slip.List
	)
	switch config.Discard {
	case jetstream.DiscardOld:
		discard = slip.Symbol(":old")
	case jetstream.DiscardNew:
		discard = slip.Symbol(":new")
	}
	for k, v := range config.Metadata {
		meta = append(meta, slip.String(k), slip.String(v))
	}
	if config.Mirror != nil {
		mirror = MakeStreamSource(config.Mirror)
	}
	if config.Placement != nil {
		placement = append(placement, slip.String(config.Placement.Cluster))
		for _, tag := range config.Placement.Tags {
			placement = append(placement, slip.String(tag))
		}
	}
	if config.RePublish != nil {
		repub = slip.List{
			slip.String(config.RePublish.Source),
			slip.String(config.RePublish.Destination),
			slipBool(config.RePublish.HeadersOnly),
		}
	}
	switch config.Retention {
	case jetstream.LimitsPolicy:
		retention = slip.Symbol(":limit")
	case jetstream.InterestPolicy:
		retention = slip.Symbol(":interest")
	case jetstream.WorkQueuePolicy:
		retention = slip.Symbol(":queue")
	}
	for _, src := range config.Sources {
		sources = append(sources, MakeStreamSource(src))
	}
	switch config.Storage {
	case jetstream.FileStorage:
		storage = slip.Symbol(":file")
	case jetstream.MemoryStorage:
		storage = slip.Symbol(":memory")
	}
	if config.SubjectTransform != nil {
		transform = slip.List{
			slip.String(config.SubjectTransform.Source),
			slip.String(config.SubjectTransform.Destination),
		}
	}
	for _, subj := range config.Subjects {
		subjects = append(subjects, slip.String(subj))
	}
	return slip.List{
		slip.Symbol(":name"), slip.String(config.Name),
		slip.Symbol(":allow-direct"), slipBool(config.AllowDirect),
		slip.Symbol(":allow-rollup"), slipBool(config.AllowRollup),
		slip.Symbol(":compression"), slipBool(config.Compression == jetstream.S2Compression),
		slip.Symbol(":consumer-limits"), slip.List{
			slip.DoubleFloat(float64(config.ConsumerLimits.InactiveThreshold) / float64(time.Second)),
			slip.Fixnum(config.ConsumerLimits.MaxAckPending),
		},
		slip.Symbol(":deny-delete"), slipBool(config.DenyDelete),
		slip.Symbol(":deny-purge"), slipBool(config.DenyPurge),
		slip.Symbol(":description"), slip.String(config.Description),
		slip.Symbol(":discard"), discard,
		slip.Symbol(":discard-new-per-subject"), slipBool(config.DiscardNewPerSubject),
		slip.Symbol(":duplicates"), slip.DoubleFloat(float64(config.Duplicates) / float64(time.Second)),
		slip.Symbol(":first-seq"), slip.Fixnum(config.FirstSeq),
		slip.Symbol(":max-age"), slip.DoubleFloat(float64(config.MaxAge) / float64(time.Second)),
		slip.Symbol(":max-bytes"), slip.Fixnum(config.MaxBytes),
		slip.Symbol(":max-consumers"), slip.Fixnum(config.MaxConsumers),
		slip.Symbol(":max-msg-size"), slip.Fixnum(config.MaxMsgSize),
		slip.Symbol(":max-msgs"), slip.Fixnum(config.MaxMsgs),
		slip.Symbol(":max-msgs-per-subject"), slip.Fixnum(config.MaxMsgsPerSubject),
		slip.Symbol(":metadata"), meta,
		slip.Symbol(":mirror"), mirror,
		slip.Symbol(":mirror-direct"), slipBool(config.MirrorDirect),
		slip.Symbol(":no-ack"), slipBool(config.NoAck),
		slip.Symbol(":placement"), placement,
		slip.Symbol(":re-publish"), repub,
		slip.Symbol(":replicas"), slip.Fixnum(config.Replicas),
		slip.Symbol(":retention"), retention,
		slip.Symbol(":sealed"), slipBool(config.Sealed),
		slip.Symbol(":sources"), sources,
		slip.Symbol(":storage"), storage,
		slip.Symbol(":subject-transform"), transform,
		slip.Symbol(":subjects"), subjects,
	}
}

// func makeFuncArgs() (args []*slip.DocArg) {
// 	args = make([]*slip.DocArg, len(streamOptMap)+1)
// 	keys := make([]string, 0, len(streamOptMap))
// 	for k := range streamOptMap {
// 		keys = append(keys, k)
// 	}
// 	sort.Strings(keys)
// 	args[0] = &slip.DocArg{Name: "&key"}
// 	for i, k := range keys {
// 		args[i+1] = streamOptMap[k].doc
// 	}
// 	return
// }

func makeStreamMethodDoc(method, args, retType, argDocs, description string) string {
	var b []byte
	b = append(b, "__"...)
	b = append(b, method...)
	b = append(b, "__ "...)
	b = append(b, args...)
	b = append(b, "&key"...)
	keys := make([]string, 0, len(streamOptMap))
	for k := range streamOptMap {
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
	for _, k := range keys {
		b = append(b, "\n   _"...)
		b = append(b, k...)
		b = append(b, "_ ["...)
		doc := streamOptMap[k].doc
		b = append(b, doc.Type...)
		b = append(b, "] "...)
		b = append(b, doc.Text...)
	}
	b = append(b, '\n', '\n', '\n')
	b = append(b, description...)
	b = append(b, '\n')

	return string(b)
}

func slipBool(v bool) slip.Object {
	if v {
		return slip.True
	}
	return nil
}
