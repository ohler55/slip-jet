// Copyright (c) 2024, Peter Ohler, All rights reserved.

package jet

import (
	"time"

	"github.com/nats-io/nats.go/jetstream"
	"github.com/ohler55/slip"
	"github.com/ohler55/slip/pkg/flavors"
)

var (
	streamSourceFlavor *flavors.Flavor
)

func defStreamSource() {
	streamSourceFlavor = flavors.DefFlavor("jet-stream-source",
		map[string]slip.Object{
			"name":                    nil, // string
			"start-seq":               nil, // int
			"start-time":              nil, // time
			"filter":                  nil, // subject string
			"transforms":              nil, // assoc of (source . destination)
			"external-prefix":         nil, // string
			"external-deliver-prefix": nil, // string
			"domain":                  nil, // string
		},
		[]string{},
		slip.List{
			slip.Symbol(":gettable-instance-variables"),
			slip.Symbol(":settable-instance-variables"),
			slip.Symbol(":inittable-instance-variables"),
			slip.List{
				slip.Symbol(":documentation"),
				slip.String(`An instance of the __jet-stream-source__ flavor describes a stream source.`),
			},
		},
		&Pkg,
	)
	streamSourceFlavor.Document("name", "the name of the stream to source from.")
	streamSourceFlavor.Document("start-seq", "the sequence number to start sourcing from.")
	streamSourceFlavor.Document("start-time", "the timestamp of messages to start sourcing from.")
	streamSourceFlavor.Document("filter", "the subject filter used to only replicate messages with matching subjects.")
	streamSourceFlavor.Document("transforms",
		`an association list of subject transforms (source . destination) to apply to matching messages.
Subject transforms on sources and mirrors are also used as subject filters with optional transformations.`)
	streamSourceFlavor.Document("external-prefix",
		"a configuration referencing a stream source in another account or JetStream domain.")
	streamSourceFlavor.Document("external-delivery-prefix",
		`used to configure a stream source in another JetStream domain. This setting will set
the External field with the appropriate APIPrefix.`)
	streamSourceFlavor.Document("domain",
		`used to configure a stream source in another JetStream domain. This setting will set
the External field with the appropriate APIPrefix.`)
}

// MakeStreamSource makes a jet-stream-source.
func MakeStreamSource(jss *jetstream.StreamSource) (inst *flavors.Instance) {
	inst = streamSourceFlavor.MakeInstance().(*flavors.Instance)
	if jss != nil {
		inst.UnsafeLet(slip.Symbol("name"), slip.String(jss.Name))
		inst.UnsafeLet(slip.Symbol("start-seq"), slip.Fixnum(jss.OptStartSeq))
		if jss.OptStartTime != nil {
			inst.UnsafeLet(slip.Symbol("start-time"), slip.Time(*jss.OptStartTime))
		}
		inst.UnsafeLet(slip.Symbol("filter"), slip.String(jss.FilterSubject))
		if 0 < len(jss.SubjectTransforms) {
			var transforms slip.List
			for _, st := range jss.SubjectTransforms {
				transforms = append(transforms,
					slip.List{slip.String(st.Source), slip.Tail{Value: slip.String(st.Destination)}})
			}
			inst.UnsafeLet(slip.Symbol("transforms"), transforms)
		}
		if jss.External != nil {
			inst.UnsafeLet(slip.Symbol("external-prefix"), slip.String(jss.External.APIPrefix))
			inst.UnsafeLet(slip.Symbol("external-deliver-prefix"), slip.String(jss.External.DeliverPrefix))
		}
		inst.UnsafeLet(slip.Symbol("domain"), slip.String(jss.Domain))
	}
	return
}

// SetJetstreamStreamSource sets the fields in a jetstream.StreamSource from
// the instance variables in an instance.
func SetJetstreamStreamSource(jss *jetstream.StreamSource, inst *flavors.Instance) {
	if obj, _ := inst.LocalGet(slip.Symbol("name")); obj != nil {
		jss.Name = slip.MustBeString(obj, "name")
	}
	if obj, _ := inst.LocalGet(slip.Symbol("start-seq")); obj != nil {
		if num, ok := obj.(slip.Fixnum); ok {
			jss.OptStartSeq = uint64(num)
		}
	}
	if obj, _ := inst.LocalGet(slip.Symbol("start-time")); obj != nil {
		if tm, ok := obj.(slip.Time); ok {
			tt := time.Time(tm)
			jss.OptStartTime = &tt
		}
	}
	if obj, _ := inst.LocalGet(slip.Symbol("filter")); obj != nil {
		jss.FilterSubject = slip.MustBeString(obj, "filter")
	}
	if obj, _ := inst.LocalGet(slip.Symbol("transforms")); obj != nil {
		if assoc, ok := obj.(slip.List); ok {
			for _, a := range assoc {
				if cons, ok2 := a.(slip.List); ok2 {
					jss.SubjectTransforms = append(jss.SubjectTransforms,
						jetstream.SubjectTransformConfig{
							Source:      slip.MustBeString(cons.Car(), "transform car"),
							Destination: slip.MustBeString(cons.Cdr(), "transform cdr"),
						})
				}
			}
		}
	}
	var x jetstream.ExternalStream
	if obj, _ := inst.LocalGet(slip.Symbol("external-prefix")); obj != nil {
		x.APIPrefix = slip.MustBeString(obj, "external-prefix")
		jss.External = &x
	}
	if obj, _ := inst.LocalGet(slip.Symbol("external-deliver-prefix")); obj != nil {
		x.DeliverPrefix = slip.MustBeString(obj, "external-deliver-prefix")
		jss.External = &x
	}
	if obj, _ := inst.LocalGet(slip.Symbol("domain")); obj != nil {
		jss.Domain = slip.MustBeString(obj, "domain")
	}
}
