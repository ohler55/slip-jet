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
	consumerFlavor *flavors.Flavor
)

func defConsumer() {
	consumerFlavor = flavors.DefFlavor("jet-consumer",
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
	consumerFlavor.DefMethod(":init", "", consumerInitCaller{})
	// fetch
	// fetch-bytes
	// fetch-no-wait
	// consume
	// next
	// info
	// cache-info

}

type consumer struct {
	self *flavors.Instance
}

type consumerInitCaller struct{}

func (caller consumerInitCaller) Call(s *slip.Scope, args slip.List, _ int) slip.Object {
	obj := s.Get("self").(*flavors.Instance)
	if 0 < len(args) {
		args = args[0].(slip.List)
	}
	c := consumer{self: obj}
	obj.Any = &c

	fmt.Printf("*** init args: %s\n", args)
	// TBD

	return nil
}

func (caller consumerInitCaller) Docs() string {
	return `__:init__


Sets the initial value when _make-instance_ is called.
`
}
