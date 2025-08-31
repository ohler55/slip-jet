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
	flavors.FlosFun("jet-client-close", ":close", clientCloseCaller{}.FuncDocs(), &Pkg)

	clientFlavor.DefMethod(":options", "", optionsCaller{})
	flavors.FlosFun("jet-client-options", ":options", optionsCaller{}.FuncDocs(), &Pkg)

	clientFlavor.DefMethod(":publish", "", publishCaller{})
	flavors.FlosFun("jet-publish", ":publish", publishCaller{}.FuncDocs(), &Pkg)

	clientFlavor.DefMethod(":publish-async", "", publishAsyncCaller{})
	flavors.FlosFun("jet-publish-async", ":publish-async", publishAsyncCaller{}.FuncDocs(), &Pkg)

	clientFlavor.DefMethod(":publish-pending", "", publishPendingCaller{})
	flavors.FlosFun("jet-publish-pending", ":publish-pending", publishPendingCaller{}.FuncDocs(), &Pkg)

	clientFlavor.DefMethod(":publish-complete", "", publishCompleteCaller{})
	flavors.FlosFun("jet-publish-complete", ":publish-complete", publishCompleteCaller{}.FuncDocs(), &Pkg)

	clientFlavor.DefMethod(":cleanup-publisher", "", cleanupPublisherCaller{})
	flavors.FlosFun("jet-cleanup-publisher", ":cleanup-publisher", cleanupPublisherCaller{}.FuncDocs(), &Pkg)

	clientFlavor.DefMethod(":create-stream", "", createStreamCaller{})
	flavors.FlosFun("jet-create-stream", ":create-stream", createStreamCaller{}.FuncDocs(), &Pkg)

	clientFlavor.DefMethod(":update-stream", "", updateStreamCaller{})
	flavors.FlosFun("jet-update-stream", ":update-stream", updateStreamCaller{}.FuncDocs(), &Pkg)

	clientFlavor.DefMethod(":create-or-update-stream", "", createOrUpdateStreamCaller{})
	flavors.FlosFun("jet-create-or-update-stream", ":create-or-update-stream",
		createOrUpdateStreamCaller{}.FuncDocs(), &Pkg)

	clientFlavor.DefMethod(":stream", "", clientStreamCaller{})
	flavors.FlosFun("jet-stream", ":stream", clientStreamCaller{}.FuncDocs(), &Pkg)

	clientFlavor.DefMethod(":stream-name-by-subject", "", streamNameBySubjectCaller{})
	flavors.FlosFun("jet-stream-name-by-subject", ":stream-name-by-subject",
		streamNameBySubjectCaller{}.FuncDocs(), &Pkg)

	clientFlavor.DefMethod(":list-streams", "", listStreamsCaller{})
	flavors.FlosFun("jet-list-streams", ":list-streams", listStreamsCaller{}.FuncDocs(), &Pkg)

	clientFlavor.DefMethod(":stream-names", "", streamNamesCaller{})
	flavors.FlosFun("jet-stream-names", ":stream-names", streamNamesCaller{}.FuncDocs(), &Pkg)

	clientFlavor.DefMethod(":delete-stream", "", deleteStreamCaller{})
	flavors.FlosFun("jet-delete-stream", ":delete-stream", deleteStreamCaller{}.FuncDocs(), &Pkg)

	clientFlavor.DefMethod(":create-consumer", "", clientCreateConsumerCaller{})
	flavors.FlosFun("jet-client-create-consumer", ":create-consumer", clientCreateConsumerCaller{}.FuncDocs(), &Pkg)

	clientFlavor.DefMethod(":update-consumer", "", clientUpdateConsumerCaller{})
	flavors.FlosFun("jet-client-update-consumer", ":update-consumer", clientUpdateConsumerCaller{}.FuncDocs(), &Pkg)

	clientFlavor.DefMethod(":create-or-update-consumer", "", clientCreateOrUpdateConsumerCaller{})
	flavors.FlosFun("jet-client-create-or-update-consumer", ":create-or-update-consumer",
		clientCreateOrUpdateConsumerCaller{}.FuncDocs(), &Pkg)

	clientFlavor.DefMethod(":ordered-consumer", "", clientOrderedConsumerCaller{})
	flavors.FlosFun("jet-client-ordered-consumer", ":ordered-consumer", clientOrderedConsumerCaller{}.FuncDocs(), &Pkg)

	clientFlavor.DefMethod(":consumer", "", clientConsumerCaller{})
	flavors.FlosFun("jet-client-consumer", ":consumer", clientConsumerCaller{}.FuncDocs(), &Pkg)

	clientFlavor.DefMethod(":delete-consumer", "", clientDeleteConsumerCaller{})
	flavors.FlosFun("jet-client-delete-consumer", ":delete-consumer", clientDeleteConsumerCaller{}.FuncDocs(), &Pkg)

	clientFlavor.DefMethod(":create-push-consumer", "", clientCreatePushConsumerCaller{})
	flavors.FlosFun("jet-client-create-push-consumer", ":create-push-consumer",
		clientCreatePushConsumerCaller{}.FuncDocs(), &Pkg)

	clientFlavor.DefMethod(":pause-consumer", "", clientPauseConsumerCaller{})
	flavors.FlosFun("jet-client-pause-consumer", ":pause-consumer", clientPauseConsumerCaller{}.FuncDocs(), &Pkg)

	clientFlavor.DefMethod(":resume-consumer", "", clientResumeConsumerCaller{})
	flavors.FlosFun("jet-client-resume-consumer", ":resume-consumer", clientResumeConsumerCaller{}.FuncDocs(), &Pkg)

	clientFlavor.DefMethod(":account-info", "", accountInfoCaller{})
	flavors.FlosFun("jet-account-info", ":account-info", accountInfoCaller{}.FuncDocs(), &Pkg)
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
