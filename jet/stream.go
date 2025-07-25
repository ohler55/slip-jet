// Copyright (c) 2024, Peter Ohler, All rights reserved.

package jet

import (
	"github.com/ohler55/slip"
	"github.com/ohler55/slip/pkg/flavors"

	_ "github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
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
	// streamFlavor.GoMakeOnly = true

	streamFlavor.DefMethod(":info", "", streamInfoCaller{})
	flavors.FlosFun("jet-stream-info", ":info", streamInfoCaller{}.FuncDocs(), &Pkg)

	streamFlavor.DefMethod(":name", "", streamNameCaller{})
	flavors.FlosFun("jet-stream-name", ":name", streamNameCaller{}.FuncDocs(), &Pkg)

	streamFlavor.DefMethod(":subjects", "", streamSubjectsCaller{})
	flavors.FlosFun("jet-stream-subjects", ":subjects", streamSubjectsCaller{}.FuncDocs(), &Pkg)

	streamFlavor.DefMethod(":purge", "", streamPurgeCaller{})
	flavors.FlosFun("jet-stream-purge", ":purge", streamPurgeCaller{}.FuncDocs(), &Pkg)

	streamFlavor.DefMethod(":get-msg", "", streamGetMsgCaller{})
	flavors.FlosFun("jet-stream-get-msg", ":get-msg", streamGetMsgCaller{}.FuncDocs(), &Pkg)

	streamFlavor.DefMethod(":get-last-msg", "", streamGetLastMsgCaller{})
	flavors.FlosFun("jet-stream-get-last-msg", ":get-last-msg", streamGetLastMsgCaller{}.FuncDocs(), &Pkg)

	streamFlavor.DefMethod(":delete-msg", "", streamDeleteMsgCaller{})
	flavors.FlosFun("jet-stream-delete-msg", ":delete-msg", streamDeleteMsgCaller{}.FuncDocs(), &Pkg)

	streamFlavor.DefMethod(":create-consumer", "", streamCreateConsumerCaller{})
	flavors.FlosFun("jet-stream-create-consumer", ":create-consumer", streamCreateConsumerCaller{}.FuncDocs(), &Pkg)

	streamFlavor.DefMethod(":update-consumer", "", streamUpdateConsumerCaller{})
	flavors.FlosFun("jet-stream-update-consumer", ":update-consumer", streamUpdateConsumerCaller{}.FuncDocs(), &Pkg)

	streamFlavor.DefMethod(":create-or-update-consumer", "", streamCreateOrUpdateConsumerCaller{})
	flavors.FlosFun("jet-stream-create-or-update-consumer", ":create-or-update-consumer",
		streamCreateOrUpdateConsumerCaller{}.FuncDocs(), &Pkg)

	streamFlavor.DefMethod(":ordered-consumer", "", streamOrderedConsumerCaller{})
	flavors.FlosFun("jet-stream-ordered-consumer", ":ordered-consumer", streamOrderedConsumerCaller{}.FuncDocs(), &Pkg)

	streamFlavor.DefMethod(":get-consumer", "", streamGetConsumerCaller{})
	flavors.FlosFun("jet-stream-get-consumer", ":get-consumer", streamGetConsumerCaller{}.FuncDocs(), &Pkg)

	streamFlavor.DefMethod(":list-consumers", "", streamListConsumersCaller{})
	flavors.FlosFun("jet-stream-list-consumers", ":list-consumers", streamListConsumersCaller{}.FuncDocs(), &Pkg)

	streamFlavor.DefMethod(":consumer-names", "", streamConsumerNamesCaller{})
	flavors.FlosFun("jet-stream-consumer-names", ":consumer-names", streamConsumerNamesCaller{}.FuncDocs(), &Pkg)

	streamFlavor.DefMethod(":delete-consumer", "", streamDeleteConsumerCaller{})
	flavors.FlosFun("jet-stream-delete-consumer", ":delete-consumer", streamDeleteConsumerCaller{}.FuncDocs(), &Pkg)
}

// MakeStream makes a jet-stream.
func MakeStream(stream jetstream.Stream) (inst *flavors.Instance) {
	inst = streamFlavor.MakeInstance().(*flavors.Instance)
	inst.Any = stream

	return
}
