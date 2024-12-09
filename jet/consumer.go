// Copyright (c) 2024, Peter Ohler, All rights reserved.

package jet

import (
	"github.com/ohler55/slip"
	"github.com/ohler55/slip/pkg/flavors"

	"github.com/nats-io/nats.go/jetstream"
)

var (
	consumerFlavor *flavors.Flavor
)

func defConsumer() {
	consumerFlavor = flavors.DefFlavor("jet-consumer",
		map[string]slip.Object{},
		[]string{},
		slip.List{
			slip.List{
				slip.Symbol(":documentation"),
				slip.String(`Consumer contains methods for fetching/processing messages from
a stream, as well as fetching consumer info.
`),
			},
		},
		&Pkg,
	)
	consumerFlavor.Final = true
	consumerFlavor.GoMakeOnly = true
	// fetch
	// fetch-bytes
	// fetch-no-wait
	// consume
	// next
	// info
	// cache-info

}

// MakeConsumer makes a jet-stream.
func MakeConsumer(consumer jetstream.Consumer) (inst *flavors.Instance) {
	inst = consumerFlavor.MakeInstance().(*flavors.Instance)
	inst.Any = consumer

	return
}
