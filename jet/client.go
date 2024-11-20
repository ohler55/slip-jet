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
	keywords = append(keywords, slip.Symbol(":init-keywords"), slip.Symbol(":prefix"))
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

	clientFlavor.DefMethod(":options", "", clientOptionsCaller{})
	flavors.FlosFun("jet-client-options", ":options", clientOptionsCaller{}.Docs(), &Pkg)

	clientFlavor.DefMethod(":publish", "", clientPublishCaller{})
	flavors.FlosFun("jet-publish", ":publish", clientPublishCaller{}.Docs(), &Pkg)

	// TBD
}

// Client is a container for the elements needed by an instance of the
// jet-client flavor.
type Client struct {
	nc      *nats.Conn
	js      jetstream.JetStream
	options slip.List
}

// NatsConn returns the nats.Conn member of the client.
func (cl *Client) NatsConn() *nats.Conn {
	return cl.nc
}

// JetStream returns the jetstream.JetStream member of the client.
func (cl *Client) JetStream() jetstream.JetStream {
	return cl.js
}
