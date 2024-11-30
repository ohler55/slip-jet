// Copyright (c) 2024, Peter Ohler, All rights reserved.

package jet

import (
	"github.com/ohler55/slip"
	"github.com/ohler55/slip/pkg/flavors"
)

type publishCompleteCaller struct{}

func (caller publishCompleteCaller) Call(s *slip.Scope, args slip.List, _ int) slip.Object {
	self := s.Get("self").(*flavors.Instance)
	flavors.CheckMethodArgCount(self, ":publish-complete", len(args), 0, 0)
	cl, ok := self.Any.(*Client)
	if !ok || cl.nc == nil {
		slip.NewPanic("%s is not a connected jet-client", self)
	}
	return TChannel(cl.js.PublishAsyncComplete())
}

func (caller publishCompleteCaller) Docs() string {
	return `__:publish-complete__ => _channel_


Returns a channel that will be closed when all outstanding asynchronously
published messages are acknowledged by the server.
`
}
