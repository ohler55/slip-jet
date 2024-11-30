// Copyright (c) 2024, Peter Ohler, All rights reserved.

package jet

import (
	"fmt"

	"github.com/ohler55/slip"
	"github.com/ohler55/slip/pkg/flavors"

	_ "github.com/nats-io/nats.go"
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
				slip.String(`
TBD

`),
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
	streamFlavor.DefMethod(":init", "", streamInitCaller{})
	// info &key cached timeout ... other options
	// purge &key timeout ... other options
	// get-msg seq &key subject timeout ... other options
	// delete-msg seq &key secure ... other options

}

type stream struct {
	self *flavors.Instance
}

type streamInitCaller struct{}

func (caller streamInitCaller) Call(s *slip.Scope, args slip.List, _ int) slip.Object {
	obj := s.Get("self").(*flavors.Instance)
	if 0 < len(args) {
		args = args[0].(slip.List)
	}
	st := stream{self: obj}
	obj.Any = &st

	fmt.Printf("*** init args: %s\n", args)
	// TBD

	return nil
}

func (caller streamInitCaller) Docs() string {
	return `__:init__


Sets the initial value when _make-instance_ is called.
`
}
