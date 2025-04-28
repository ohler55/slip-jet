// Copyright (c) 2024, Peter Ohler, All rights reserved.

package jet

import (
	"time"

	"github.com/ohler55/slip"
	"github.com/ohler55/slip/pkg/flavors"

	_ "github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

var (
	streamSourceInfoFlavor *flavors.Flavor
)

func defStreamSourceInfo() {
	streamSourceInfoFlavor = flavors.DefFlavor("jet-stream-source-info",
		map[string]slip.Object{
			"name":               nil,
			"lag":                nil,
			"active":             nil,
			"filter-subject":     nil,
			"subject-transforms": nil,
		},
		[]string{},
		slip.List{
			slip.List{
				slip.Symbol(":documentation"),
				slip.String(`jet-stream-source-info is information about a stream source.`),
			},
			slip.Symbol(":gettable-instance-variables"),
			slip.Symbol(":settable-instance-variables"),
			slip.Symbol(":inittable-instance-variables"),
		},
		&Pkg,
	)
	streamSourceInfoFlavor.Document("name", "The name of the stream that is being replicated.")
	streamSourceInfoFlavor.Document("lag", `How many messages behind the source/mirror operation is.
This will only show correctly if there is active communication with stream/mirror.`)
	streamSourceInfoFlavor.Document("active", `When last the mirror or sourced stream had activity.
Value will be _nil_ when there has been no activity.`)
	streamSourceInfoFlavor.Document("filter-subject", "The subject filter defined for this source/mirror.")
	streamSourceInfoFlavor.Document("subject-transforms",
		"A property list of subject transforms defined for this source/mirror.")
}

// MakeStreamSourceInfo creates a jet-stream-source-info instance from a
// jetstream.StreamSourceInfo.
func MakeStreamSourceInfo(ssi *jetstream.StreamSourceInfo) (inst *flavors.Instance) {
	inst = streamSourceInfoFlavor.MakeInstance().(*flavors.Instance)
	inst.UnsafeLet(slip.Symbol("name"), slip.String(ssi.Name))
	inst.UnsafeLet(slip.Symbol("lag"), slip.Fixnum(ssi.Lag))
	if ssi.Active < 0 {
		inst.UnsafeLet(slip.Symbol("active"), nil)
	} else {
		inst.UnsafeLet(slip.Symbol("active"), slip.DoubleFloat(float64(ssi.Active)/float64(time.Second)))
	}
	inst.UnsafeLet(slip.Symbol("filter-subject"), slip.String(ssi.FilterSubject))
	transforms := make(slip.List, 0, 2*len(ssi.SubjectTransforms))
	for _, trans := range ssi.SubjectTransforms {
		transforms = append(transforms, slip.String(trans.Source), slip.String(trans.Destination))
	}
	inst.UnsafeLet(slip.Symbol("subject-transforms"), transforms)
	return
}
