// Copyright (c) 2024, Peter Ohler, All rights reserved.

package jet

import (
	"github.com/ohler55/slip"
	"github.com/ohler55/slip/pkg/flavors"

	_ "github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	_ "github.com/nats-io/nats.go/jetstream"
)

var (
	streamFlavor *flavors.Flavor
)

func defStream() {
	streamFlavor = flavors.DefFlavor("jet-stream",
		map[string]slip.Object{},
		[]string{},
		slip.List{
			slip.List{
				slip.Symbol(":documentation"),
				slip.String(`Stream contains CRUD methods on a consumer as well as operations
on an existing stream. It allows fetching and removing messages from a stream, as well as
purging a stream.`),
			},
		},
		&Pkg,
	)
	streamFlavor.Final = true
	streamFlavor.GoMakeOnly = true

	streamFlavor.DefMethod(":info", "", streamInfoCaller{})
	flavors.FlosFun("jet-stream-info", ":info", streamInfoCaller{}.Docs(), &Pkg)

	streamFlavor.DefMethod(":name", "", streamNameCaller{})
	flavors.FlosFun("jet-stream-name", ":name", streamNameCaller{}.Docs(), &Pkg)

	streamFlavor.DefMethod(":subjects", "", streamSubjectsCaller{})
	flavors.FlosFun("jet-stream-subjects", ":subjects", streamSubjectsCaller{}.Docs(), &Pkg)

	streamFlavor.DefMethod(":purge", "", streamPurgeCaller{})
	flavors.FlosFun("jet-stream-purge", ":purge", streamPurgeCaller{}.Docs(), &Pkg)

	streamFlavor.DefMethod(":get-msg", "", streamGetMsgCaller{})
	flavors.FlosFun("jet-stream-get-msg", ":get-msg", streamGetMsgCaller{}.Docs(), &Pkg)

	streamFlavor.DefMethod(":get-last-msg", "", streamGetLastMsgCaller{})
	flavors.FlosFun("jet-stream-get-last-msg", ":get-last-msg", streamGetLastMsgCaller{}.Docs(), &Pkg)

	streamFlavor.DefMethod(":delete-msg", "", streamDeleteMsgCaller{})
	flavors.FlosFun("jet-stream-delete-msg", ":delete-msg", streamDeleteMsgCaller{}.Docs(), &Pkg)

	streamFlavor.DefMethod(":create-consumer", "", streamCreateConsumerCaller{})
	flavors.FlosFun("jet-stream-create-consumer", ":create-consumer", streamCreateConsumerCaller{}.Docs(), &Pkg)

	streamFlavor.DefMethod(":update-consumer", "", streamUpdateConsumerCaller{})
	flavors.FlosFun("jet-stream-update-consumer", ":update-consumer", streamUpdateConsumerCaller{}.Docs(), &Pkg)

	streamFlavor.DefMethod(":create-or-update-consumer", "", streamCreateOrUpdateConsumerCaller{})
	flavors.FlosFun("jet-stream-create-or-update-consumer", ":create-or-update-consumer",
		streamCreateOrUpdateConsumerCaller{}.Docs(), &Pkg)

	streamFlavor.DefMethod(":ordered-consumer", "", streamOrderedConsumerCaller{})
	flavors.FlosFun("jet-stream-ordered-consumer", ":ordered-consumer", streamOrderedConsumerCaller{}.Docs(), &Pkg)

	streamFlavor.DefMethod(":get-consumer", "", streamGetConsumerCaller{})
	flavors.FlosFun("jet-stream-get-consumer", ":get-consumer", streamGetConsumerCaller{}.Docs(), &Pkg)

	streamFlavor.DefMethod(":list-consumers", "", streamListConsumersCaller{})
	flavors.FlosFun("jet-stream-list-consumers", ":list-consumers", streamListConsumersCaller{}.Docs(), &Pkg)

	streamFlavor.DefMethod(":consumer-names", "", streamConsumerNamesCaller{})
	flavors.FlosFun("jet-stream-consumer-names", ":consumer-names", streamConsumerNamesCaller{}.Docs(), &Pkg)

	// TBD
}

// MakeStream makes a jet-stream.
func MakeStream(stream jetstream.Stream) (inst *flavors.Instance) {
	inst = streamFlavor.MakeInstance().(*flavors.Instance)
	inst.Any = stream

	return
}
