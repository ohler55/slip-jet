// Copyright (c) 2024, Peter Ohler, All rights reserved.

package jet

import (
	"github.com/ohler55/slip"
	"github.com/ohler55/slip/pkg/flavors"
)

var (
	ackFlavor *flavors.Flavor
)

func defAck() {
	ackFlavor = flavors.DefFlavor("jet-ack",
		map[string]slip.Object{
			"stream-name":     nil,
			"sequence-number": nil,
			"duplicate":       nil,
			"domain":          nil,
		},
		[]string{},
		slip.List{
			slip.Symbol(":gettable-instance-variables"),
			slip.Symbol(":settable-instance-variables"),
			slip.Symbol(":inittable-instance-variables"),
			slip.List{
				slip.Symbol(":documentation"),
				slip.String(`An instance of the _jet-ack_ flavor is received after a successful publish.`),
			},
		},
		&Pkg,
	)
}

func makeAck(stream string, seq uint64, dup bool, domain string) (inst *flavors.Instance) {
	inst = ackFlavor.MakeInstance().(*flavors.Instance)
	inst.UnsafeLet(slip.Symbol("stream-name"), slip.String(stream))
	inst.UnsafeLet(slip.Symbol("sequence-number"), slip.Fixnum(seq))
	if dup {
		inst.UnsafeLet(slip.Symbol("duplicate"), slip.True)
	}
	inst.UnsafeLet(slip.Symbol("domain"), slip.String(domain))

	return
}
