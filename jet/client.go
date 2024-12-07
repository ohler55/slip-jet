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

	clientFlavor.DefMethod(":options", "", optionsCaller{})
	flavors.FlosFun("jet-client-options", ":options", optionsCaller{}.Docs(), &Pkg)

	clientFlavor.DefMethod(":publish", "", publishCaller{})
	flavors.FlosFun("jet-publish", ":publish", publishCaller{}.Docs(), &Pkg)

	clientFlavor.DefMethod(":publish-async", "", publishAsyncCaller{})
	flavors.FlosFun("jet-publish-async", ":publish-async", publishAsyncCaller{}.Docs(), &Pkg)

	clientFlavor.DefMethod(":publish-pending", "", publishPendingCaller{})
	flavors.FlosFun("jet-publish-pending", ":publish-pending", publishPendingCaller{}.Docs(), &Pkg)

	clientFlavor.DefMethod(":publish-complete", "", publishCompleteCaller{})
	flavors.FlosFun("jet-publish-complete", ":publish-complete", publishCompleteCaller{}.Docs(), &Pkg)

	clientFlavor.DefMethod(":cleanup-publisher", "", cleanupPublisherCaller{})
	flavors.FlosFun("jet-cleanup-publisher", ":cleanup-publisher", cleanupPublisherCaller{}.Docs(), &Pkg)

	clientFlavor.DefMethod(":create-stream", "", createStreamCaller{})
	flavors.FlosFun("jet-create-stream", ":create-stream", createStreamCaller{}.Docs(), &Pkg)

	clientFlavor.DefMethod(":update-stream", "", updateStreamCaller{})
	flavors.FlosFun("jet-update-stream", ":update-stream", updateStreamCaller{}.Docs(), &Pkg)

	clientFlavor.DefMethod(":create-or-update-stream", "", createOrUpdateStreamCaller{})
	flavors.FlosFun("jet-create-or-update-stream", ":create-or-update-stream",
		createOrUpdateStreamCaller{}.Docs(), &Pkg)

	clientFlavor.DefMethod(":get-stream", "", getStreamCaller{})
	flavors.FlosFun("jet-get-stream", ":get-stream", getStreamCaller{}.Docs(), &Pkg)

	clientFlavor.DefMethod(":stream-name-by-subject", "", streamNameBySubjectCaller{})
	flavors.FlosFun("jet-stream-name-by-subject", ":stream-name-by-subject", streamNameBySubjectCaller{}.Docs(), &Pkg)

	clientFlavor.DefMethod(":list-streams", "", listStreamsCaller{})
	flavors.FlosFun("jet-list-streams", ":list-streams", listStreamsCaller{}.Docs(), &Pkg)

	clientFlavor.DefMethod(":stream-names", "", streamNamesCaller{})
	flavors.FlosFun("jet-stream-names", ":stream-names", streamNamesCaller{}.Docs(), &Pkg)

	clientFlavor.DefMethod(":delete-stream", "", deleteStreamCaller{})
	flavors.FlosFun("jet-delete-stream", ":delete-stream", deleteStreamCaller{}.Docs(), &Pkg)
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
