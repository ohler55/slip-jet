// Copyright (c) 2024, Peter Ohler, All rights reserved.

package jet

import (
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/ohler55/slip"
	"github.com/ohler55/slip/pkg/flavors"
)

var (
	clientFlavor *flavors.Flavor
)

func defClient() {
	keywords := make(slip.List, 0, len(conOptMap)+1)
	keywords = append(keywords, slip.Symbol(":init-keywords"))
	for k := range conOptMap {
		keywords = append(keywords, slip.Symbol(k))
	}
	clientFlavor = flavors.DefFlavor("jet-client",
		map[string]slip.Object{},
		[]string{},
		slip.List{
			slip.List{
				slip.Symbol(":documentation"),
				slip.String(`Is a connection to a NATS JetStream server.`),
			},
			keywords,
		},
		&Pkg,
	)
	clientFlavor.DefMethod(":init", "", clientInitCaller{})

	clientFlavor.DefMethod(":close", "", clientCloseCaller{})
	flavors.FlosFun("jet-client-close", ":close", clientCloseCaller{}.Docs(), &Pkg)

	// TBD
}

type client struct {
	nc *nats.Conn
	js jetstream.JetStream
}
