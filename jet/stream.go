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
			slip.List{
				slip.Symbol(":init-keywords"),
				slip.Symbol(":name"),
			},
			slip.Symbol(":gettable-instance-variables"),
			slip.Symbol(":settable-instance-variables"),
			slip.Symbol(":inittable-instance-variables"),
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

	// :purge &key timeout keep sequence subject
	// :get-msg seq &key subject timeout
	// :get-last-msg subject &key timeout
	// :delete-msg seq &key secure
	// TBD
}

// MakeStream makes a jet-stream.
func MakeStream(stream jetstream.Stream) (inst *flavors.Instance) {
	inst = streamFlavor.MakeInstance().(*flavors.Instance)
	inst.Any = stream

	return
}
