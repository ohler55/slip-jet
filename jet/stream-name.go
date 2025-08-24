// Copyright (c) 2024, Peter Ohler, All rights reserved.

package jet

import (
	"github.com/ohler55/slip"
	"github.com/ohler55/slip/pkg/flavors"

	"github.com/nats-io/nats.go/jetstream"
)

type streamNameCaller struct{}

func (caller streamNameCaller) Call(s *slip.Scope, args slip.List, depth int) (result slip.Object) {
	self := s.Get("self").(*flavors.Instance)
	slip.MethodArgCountCheck(s, depth, self, ":name", len(args), 0, 0)
	stream := self.Any.(jetstream.Stream)
	if si := stream.CachedInfo(); si != nil {
		result = slip.String(si.Config.Name)
	}
	return
}

func (caller streamNameCaller) FuncDocs() *slip.FuncDoc {
	return &slip.FuncDoc{
		Name:   ":name",
		Text:   `Returns the cached stream name.`,
		Return: "string",
	}
}
