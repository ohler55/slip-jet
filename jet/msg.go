// Copyright (c) 2024, Peter Ohler, All rights reserved.

package jet

import (
	"github.com/ohler55/slip"
	"github.com/ohler55/slip/pkg/flavors"

	_ "github.com/nats-io/nats.go"
	_ "github.com/nats-io/nats.go/jetstream"
)

var (
	msgFlavor *flavors.Flavor
)

func defMsg() {
	msgFlavor = flavors.DefFlavor("msg",
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
	msgFlavor.DefMethod(":init", "", msgInitCaller{})
}

type msg struct {
	self *flavors.Instance
}

type msgInitCaller struct{}

func (caller msgInitCaller) Call(s *slip.Scope, args slip.List, _ int) slip.Object {
	obj := s.Get("self").(*flavors.Instance)
	if 0 < len(args) {
		args = args[0].(slip.List)
	}
	c := msg{self: obj}
	obj.Any = &c

	return nil
}

func (caller msgInitCaller) Docs() string {
	return `__:init__


Sets the initial value when _make-instance_ is called.
`
}
