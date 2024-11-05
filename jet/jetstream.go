// Copyright (c) 2024, Peter Ohler, All rights reserved.

package jet

import (
	"github.com/ohler55/slip"
	"github.com/ohler55/slip/pkg/flavors"

	_ "github.com/nats-io/nats.go"
	_ "github.com/nats-io/nats.go/jetstream"
)

var (
	jetstreamFlavor *flavors.Flavor
)

func defJetstream() {
	jetstreamFlavor = flavors.DefFlavor("jetstream",
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
	jetstreamFlavor.DefMethod(":init", "", jetInitCaller{})
}

type jet struct {
	self *flavors.Instance
}

type jetInitCaller struct{}

func (caller jetInitCaller) Call(s *slip.Scope, args slip.List, _ int) slip.Object {
	obj := s.Get("self").(*flavors.Instance)
	if 0 < len(args) {
		args = args[0].(slip.List)
	}
	js := jet{self: obj}
	obj.Any = &js

	return nil
}

func (caller jetInitCaller) Docs() string {
	return `__:init__


Sets the initial value when _make-instance_ is called.
`
}
