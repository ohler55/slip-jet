// Copyright (c) 2024, Peter Ohler, All rights reserved.

package jet

import (
	"github.com/ohler55/slip"
	"github.com/ohler55/slip/pkg/flavors"
)

type publishPendingCaller struct{}

func (caller publishPendingCaller) Call(s *slip.Scope, args slip.List, _ int) slip.Object {
	self := s.Get("self").(*flavors.Instance)
	slip.CheckMethodArgCount(self, ":publish-pending", len(args), 0, 0)
	cl := self.Any.(*Client)

	return slip.Fixnum(cl.js.PublishAsyncPending())
}

func (caller publishPendingCaller) FuncDocs() *slip.FuncDoc {
	return &slip.FuncDoc{
		Name: ":publish-pending",
		Text: `Returns the number of async publishes outstanding for this context. An
outstanding publish is one that has been sent by the publisher but has not yet
received an ack.`,
		Return: "fixnum",
	}
}
