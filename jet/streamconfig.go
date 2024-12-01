// Copyright (c) 2024, Peter Ohler, All rights reserved.

package jet

import (
	"sort"
	"time"

	"github.com/nats-io/nats.go/jetstream"
	"github.com/ohler55/slip"
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
			Type: "bool",
			Text: `Enables direct access to individual messages using direct get API. Defaults to _nil_.`,
		},
		update: func(config *jetstream.StreamConfig, v slip.Object) {
			config.AllowDirect = (v != nil)
		},
	},
	":allow-rollup": {
		doc: &slip.DocArg{
			Name: "allow-rollup",
			Type: "bool",
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
			Type: "bool [maps to none or s2]",
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
unacknowledged messages for a consumer.
`,
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
			Type: "bool",
			Text: `Restricts the ability to delete messages from a stream via  the API. Defaults to false.`,
		},
		update: func(config *jetstream.StreamConfig, v slip.Object) {
			config.DenyDelete = (v != nil)
		},
	},
	":deny-purge": {
		doc: &slip.DocArg{
			Name: "deny-purge",
			Type: "bool",
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
			Text: `x`,
		},
		update: func(config *jetstream.StreamConfig, v slip.Object) {
			// TBD
		},
	},
	":discard": {
		doc: &slip.DocArg{
			Name: "discard",
			Type: "[:old :new]",
			Text: `x`,
		},
		update: func(config *jetstream.StreamConfig, v slip.Object) {
			// TBD
		},
	},
	":discard-new-per-subject": {
		doc: &slip.DocArg{
			Name: "discard-new-per-subject",
			Type: "bool",
			Text: `x`,
		},
		update: func(config *jetstream.StreamConfig, v slip.Object) {
			// TBD
		},
	},
	":duplicates": {
		doc: &slip.DocArg{
			Name: "duplicates",
			Type: "real [time.Duration]",
			Text: `x`,
		},
		update: func(config *jetstream.StreamConfig, v slip.Object) {
			// TBD
		},
	},
	":first-seq": {
		doc: &slip.DocArg{
			Name: "first-seq",
			Type: "int",
			Text: `x`,
		},
		update: func(config *jetstream.StreamConfig, v slip.Object) {
			// TBD
		},
	},
	":max-age": {
		doc: &slip.DocArg{
			Name: "max-age",
			Type: "real [duration]",
			Text: `x`,
		},
		update: func(config *jetstream.StreamConfig, v slip.Object) {
			// TBD
		},
	},
	":max-bytes": {
		doc: &slip.DocArg{
			Name: "max-bytes",
			Type: "int",
			Text: `x`,
		},
		update: func(config *jetstream.StreamConfig, v slip.Object) {
			// TBD
		},
	},
	":max-consumers": {
		doc: &slip.DocArg{
			Name: "max-consumers",
			Type: "int",
			Text: `x`,
		},
		update: func(config *jetstream.StreamConfig, v slip.Object) {
			// TBD
		},
	},
	":max-msg-size": {
		doc: &slip.DocArg{
			Name: "max-msg-size",
			Type: "int",
			Text: `x`,
		},
		update: func(config *jetstream.StreamConfig, v slip.Object) {
			// TBD
		},
	},
	":max-msgs": {
		doc: &slip.DocArg{
			Name: "max-msgs",
			Type: "int",
			Text: `x`,
		},
		update: func(config *jetstream.StreamConfig, v slip.Object) {
			// TBD
		},
	},
	":max-msgs-per-subject": {
		doc: &slip.DocArg{
			Name: "max-msgs-per-subject",
			Type: "int",
			Text: `x`,
		},
		update: func(config *jetstream.StreamConfig, v slip.Object) {
			// TBD
		},
	},
	":metadata": {
		doc: &slip.DocArg{
			Name: "metadata",
			Type: "list [property list]",
			Text: `x`,
		},
		update: func(config *jetstream.StreamConfig, v slip.Object) {
			// TBD
		},
	},
	":mirror": {
		doc: &slip.DocArg{
			Name: "mirror",
			Type: "list [property list]",
			Text: `x`,
		},
		update: func(config *jetstream.StreamConfig, v slip.Object) {
			// TBD
		},
	},
	":mirror-direct": {
		doc: &slip.DocArg{
			Name: "mirror-direct",
			Type: "bool",
			Text: `x`,
		},
		update: func(config *jetstream.StreamConfig, v slip.Object) {
			// TBD
		},
	},
	":no-ack": {
		doc: &slip.DocArg{
			Name: "no-ack",
			Type: "bool",
			Text: `x`,
		},
		update: func(config *jetstream.StreamConfig, v slip.Object) {
			// TBD
		},
	},
	":placement": {
		doc: &slip.DocArg{
			Name: "placement",
			Type: "list of strings (cluster tags...)",
			Text: `x`,
		},
		update: func(config *jetstream.StreamConfig, v slip.Object) {
			// TBD
		},
	},
	":re-publish": {
		doc: &slip.DocArg{
			Name: "re-publish",
			Type: "list of (source destination headers-only)",
			Text: `x`,
		},
		update: func(config *jetstream.StreamConfig, v slip.Object) {
			// TBD
		},
	},
	":replicas": {
		doc: &slip.DocArg{
			Name: "replicas",
			Type: "int",
			Text: `x`,
		},
		update: func(config *jetstream.StreamConfig, v slip.Object) {
			// TBD
		},
	},
	":retention": {
		doc: &slip.DocArg{
			Name: "retention",
			Type: "[:limit :interest :queue]",
			Text: `x`,
		},
		update: func(config *jetstream.StreamConfig, v slip.Object) {
			// TBD
		},
	},
	":sealed": {
		doc: &slip.DocArg{
			Name: "sealed",
			Type: "bool",
			Text: `x`,
		},
		update: func(config *jetstream.StreamConfig, v slip.Object) {
			// TBD
		},
	},
	":sources": {
		doc: &slip.DocArg{
			Name: "sources",
			Type: "list of list [property list] or maybe jet-stream-source flavor instance",
			Text: `x`,
		},
		update: func(config *jetstream.StreamConfig, v slip.Object) {
			// TBD
		},
	},
	":storage": {
		doc: &slip.DocArg{
			Name: "storage",
			Type: "[:file :memory]",
			Text: `x`,
		},
		update: func(config *jetstream.StreamConfig, v slip.Object) {
			// TBD
		},
	},
	":subject-transform": {
		doc: &slip.DocArg{
			Name: "subject-transform",
			Type: "list of (source destination)",
			Text: `x`,
		},
		update: func(config *jetstream.StreamConfig, v slip.Object) {
			// TBD
		},
	},
	":subjects": {
		doc: &slip.DocArg{
			Name: "subjects",
			Type: "[]string",
			Text: `x`,
		},
		update: func(config *jetstream.StreamConfig, v slip.Object) {
			// TBD
		},
	},
	// TBD
}

// InitStreamConfig sets or updates the fields in a jetstream.StreamConfig
// based on the slip arguments provided.
func InitStreamConfig(config *jetstream.StreamConfig, args slip.List) {

}

// StreamConfigPropList returns a property list built from a
// jetstream.StreamConfig. The returned list is suitable as arguments to a
// stream creation.
func StreamConfigPropList(config *jetstream.StreamConfig) slip.List {

	// TBD

	return nil
}

// TBD docs from list or maybe just a list of

func makeFuncArgs() (args []*slip.DocArg) {
	args = make([]*slip.DocArg, len(streamOptMap)+1)
	keys := make([]string, 0, len(streamOptMap))
	for k := range streamOptMap {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	args[0] = &slip.DocArg{Name: "&key"}
	for i, k := range keys {
		args[i+1] = streamOptMap[k].doc
	}
	return
}
