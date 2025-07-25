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
	// consumerFlavor.GoMakeOnly = true

	consumerFlavor.DefMethod(":info", "", consumerInfoCaller{})
	flavors.FlosFun("jet-consumer-info", ":info", consumerInfoCaller{}.FuncDocs(), &Pkg)

	consumerFlavor.DefMethod(":name", "", consumerNameCaller{})
	flavors.FlosFun("jet-consumer-name", ":name", consumerNameCaller{}.FuncDocs(), &Pkg)

	consumerFlavor.DefMethod(":fetch", "", consumerFetchCaller{})
	flavors.FlosFun("jet-consumer-fetch", ":fetch", consumerFetchCaller{}.FuncDocs(), &Pkg)

	consumerFlavor.DefMethod(":fetch-bytes", "", consumerFetchBytesCaller{})
	flavors.FlosFun("jet-consumer-fetch-bytes", ":fetch-bytes", consumerFetchBytesCaller{}.FuncDocs(), &Pkg)

	consumerFlavor.DefMethod(":messages", "", consumerMessagesCaller{})
	flavors.FlosFun("jet-consumer-messages", ":messages", consumerMessagesCaller{}.FuncDocs(), &Pkg)

	consumerFlavor.DefMethod(":next", "", consumerNextCaller{})
	flavors.FlosFun("jet-consumer-next", ":next", consumerNextCaller{}.FuncDocs(), &Pkg)

	consumerFlavor.DefMethod(":consume", "", consumerConsumeCaller{})
	flavors.FlosFun("jet-consumer-consume", ":consume", consumerConsumeCaller{}.FuncDocs(), &Pkg)
}

// MakeConsumer makes a jet-stream.
func MakeConsumer(consumer jetstream.Consumer) (inst *flavors.Instance) {
	inst = consumerFlavor.MakeInstance().(*flavors.Instance)
	inst.Any = consumer

	return
}
