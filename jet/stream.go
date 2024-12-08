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

	// TBD
}

// MakeStream makes a jet-stream.
func MakeStream(stream jetstream.Stream) (inst *flavors.Instance) {
	inst = streamFlavor.MakeInstance().(*flavors.Instance)
	inst.Any = stream

	return
}
