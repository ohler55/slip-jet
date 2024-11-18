// Copyright (c) 2024, Peter Ohler, All rights reserved.

package jet

import (
	"github.com/ohler55/slip"
	"github.com/ohler55/slip/pkg/flavors"
)

type clientCloseCaller struct{}

func (caller clientCloseCaller) Call(s *slip.Scope, args slip.List, _ int) slip.Object {
	self := s.Get("self").(*flavors.Instance)
	flavors.CheckMethodArgCount(self, ":close", len(args), 0, 0)
	if cl, ok := self.Any.(*Client); ok {
		if cl.nc != nil {
			cl.nc.Close()
		}
		cl.nc = nil
		cl.js = nil
	}
	return nil
}

func (caller clientCloseCaller) Docs() string {
	return `__:close__


Closes the client connection to the NATS server.
`
}
