// Copyright (c) 2024, Peter Ohler, All rights reserved.

package jet

import (
	"github.com/ohler55/slip"
	"github.com/ohler55/slip/pkg/flavors"

	"github.com/nats-io/nats.go/jetstream"
)

type streamSubjectsCaller struct{}

func (caller streamSubjectsCaller) Call(s *slip.Scope, args slip.List, depth int) (result slip.Object) {
	self := s.Get("self").(*flavors.Instance)
	slip.MethodArgCountCheck(s, depth, self, ":subjects", len(args), 0, 0)
	stream := self.Any.(jetstream.Stream)
	if si := stream.CachedInfo(); si != nil {
		subjects := make(slip.List, len(si.Config.Subjects))
		for i, subj := range si.Config.Subjects {
			subjects[i] = slip.String(subj)
		}
		result = subjects
	}
	return
}

func (caller streamSubjectsCaller) FuncDocs() *slip.FuncDoc {
	return &slip.FuncDoc{
		Name:   ":subjects",
		Text:   `Returns the cached stream subjects.`,
		Return: "list",
	}
}
