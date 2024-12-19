// Copyright (c) 2024, Peter Ohler, All rights reserved.

package jet

import (
	"github.com/ohler55/slip"
	"github.com/ohler55/slip/pkg/flavors"

	"github.com/nats-io/nats.go/jetstream"
)

type consumerNameCaller struct{}

func (caller consumerNameCaller) Call(s *slip.Scope, args slip.List, _ int) (result slip.Object) {
	self := s.Get("self").(*flavors.Instance)
	flavors.CheckMethodArgCount(self, ":name", len(args), 0, 0)
	consumer := self.Any.(jetstream.Consumer)
	if si := consumer.CachedInfo(); si != nil {
		result = slip.String(si.Config.Name)
	}
	return
}

func (caller consumerNameCaller) Docs() string {
	return `__:name__ => _string_


Returns the cached consumer name.
`
}
