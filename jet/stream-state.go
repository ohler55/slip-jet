// Copyright (c) 2024, Peter Ohler, All rights reserved.

package jet

import (
	"github.com/ohler55/slip"
	"github.com/ohler55/slip/pkg/flavors"

	_ "github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	_ "github.com/nats-io/nats.go/jetstream"
)

var (
	streamStateFlavor *flavors.Flavor
)

func defStreamState() {
	streamStateFlavor = flavors.DefFlavor("jet-stream-state",
		map[string]slip.Object{
			"msgs":            nil,
			"bytes":           nil,
			"first-seq":       nil,
			"first-time":      nil,
			"last-seq":        nil,
			"last-time":       nil,
			"consumers":       nil,
			"deleted":         nil,
			"number-deleted":  nil,
			"number-subjects": nil,
			"subjects":        nil,
		},
		[]string{},
		slip.List{
			slip.List{
				slip.Symbol(":documentation"),
				slip.String(`jet-stream-state is the state of a _jet-stream_ at the time of creation.`),
			},
			slip.Symbol(":gettable-instance-variables"),
			slip.Symbol(":settable-instance-variables"),
			slip.Symbol(":inittable-instance-variables"),
		},
		&Pkg,
	)
	streamStateFlavor.Document("msgs", "The number of messages stored in the stream.")
	streamStateFlavor.Document("bytes", "The number of bytes stored in the stream.")
	streamStateFlavor.Document("first-seq", "The sequence number of the first message in the stream.")
	streamStateFlavor.Document("first-time", "The timestamp of the first message in the stream.")
	streamStateFlavor.Document("last-seq", "The sequence number of the last message in the stream.")
	streamStateFlavor.Document("last-time", "The timestamp of the last message in the stream.")
	streamStateFlavor.Document("consumers", "The number of consumers on the stream.")
	streamStateFlavor.Document("deleted", `List of sequence numbers that have been removed from the
stream. This field will only be returned if the stream has been fetched with the _:deleted_ option.`)
	streamStateFlavor.Document("number-deleted", `The number of messages that have been removed from the
stream. Only deleted messages causing a gap in stream sequence numbers are counted. Messages deleted
at the beginning or end of the stream are not counted.`)
	streamStateFlavor.Document("number-subjects", "The number of unique subjects the stream has received messages on.")
	streamStateFlavor.Document("subjects", `A property list of subjects the stream has received messages on
with message count per subject. This field will only be returned if
the stream has been fetched with the _:filter_ option.`)
}

// MakeStreamState creates a jet-stream-state instance from a jetstream.StreamState.
func MakeStreamState(ss *jetstream.StreamState) (inst *flavors.Instance) {
	inst = streamStateFlavor.MakeInstance().(*flavors.Instance)
	inst.UnsafeLet(slip.Symbol("msgs"), slip.Fixnum(ss.Msgs))
	inst.UnsafeLet(slip.Symbol("bytes"), slip.Fixnum(ss.Bytes))
	inst.UnsafeLet(slip.Symbol("first-seq"), slip.Fixnum(ss.FirstSeq))
	inst.UnsafeLet(slip.Symbol("first-time"), slip.Time(ss.FirstTime))
	inst.UnsafeLet(slip.Symbol("last-seq"), slip.Fixnum(ss.LastSeq))
	inst.UnsafeLet(slip.Symbol("last-time"), slip.Time(ss.LastTime))
	inst.UnsafeLet(slip.Symbol("consumers"), slip.Fixnum(ss.Consumers))
	deleted := make(slip.List, len(ss.Deleted))
	for i, d := range ss.Deleted {
		deleted[i] = slip.Fixnum(d)
	}
	inst.UnsafeLet(slip.Symbol("deleted"), deleted)
	inst.UnsafeLet(slip.Symbol("number-deleted"), slip.Fixnum(ss.NumDeleted))
	inst.UnsafeLet(slip.Symbol("number-subjects"), slip.Fixnum(ss.NumSubjects))
	subjects := make(slip.List, 0, 2*len(ss.Subjects))
	for subj, count := range ss.Subjects {
		subjects = append(subjects, slip.String(subj), slip.Fixnum(count))
	}
	inst.UnsafeLet(slip.Symbol("subjects"), subjects)
	return
}
