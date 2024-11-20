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
		map[string]slip.Object{},
		[]string{},
		slip.List{
			slip.List{
				slip.Symbol(":documentation"),
				slip.String(`TBD.`),
			},
		},
		&Pkg,
	)
}
