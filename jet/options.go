// Copyright (c) 2024, Peter Ohler, All rights reserved.

package jet

import (
	"github.com/ohler55/slip"
	"github.com/ohler55/slip/pkg/flavors"
)

type clientOptionsCaller struct{}

func (caller clientOptionsCaller) Call(s *slip.Scope, args slip.List, _ int) slip.Object {
	self := s.Get("self").(*flavors.Instance)
	flavors.CheckMethodArgCount(self, ":options", len(args), 0, 0)
	if cl, ok := self.Any.(*client); ok && cl.nc != nil {
		return cl.options
	}
	return nil
}

func (caller clientOptionsCaller) Docs() string {
	return `__:options__ => _property-list_


Returns the options used for the client connection as a property list.
`
}
