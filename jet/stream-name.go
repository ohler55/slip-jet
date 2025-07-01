// Copyright (c) 2024, Peter Ohler, All rights reserved.

package jet

import (
	"github.com/ohler55/slip"
	"github.com/ohler55/slip/pkg/flavors"

	"github.com/nats-io/nats.go/jetstream"
)

type streamNameCaller struct{}

func (caller streamNameCaller) Call(s *slip.Scope, args slip.List, _ int) (result slip.Object) {
	self := s.Get("self").(*flavors.Instance)
	slip.CheckMethodArgCount(self, ":name", len(args), 0, 0)
	stream := self.Any.(jetstream.Stream)
	if si := stream.CachedInfo(); si != nil {
		result = slip.String(si.Config.Name)
	}
	return
}

func (caller streamNameCaller) Docs() string {
	return `__:name__ => _string_


Returns the cached stream name.
`
}
