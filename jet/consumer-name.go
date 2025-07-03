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
	slip.CheckMethodArgCount(self, ":name", len(args), 0, 0)
	consumer := self.Any.(jetstream.Consumer)
	if si := consumer.CachedInfo(); si != nil {
		result = slip.String(si.Config.Name)
	}
	return
}

func (caller consumerNameCaller) FuncDocs() *slip.FuncDoc {
	return &slip.FuncDoc{
		Name:   ":name",
		Text:   `Returns the cached consumer name.`,
		Return: "string",
	}
}
