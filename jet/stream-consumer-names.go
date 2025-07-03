// Copyright (c) 2024, Peter Ohler, All rights reserved.

package jet

import (
	"context"

	"github.com/nats-io/nats.go/jetstream"
	"github.com/ohler55/slip"
	"github.com/ohler55/slip/pkg/flavors"
)

type streamConsumerNamesCaller struct{}

func (caller streamConsumerNamesCaller) Call(s *slip.Scope, args slip.List, _ int) slip.Object {
	self := s.Get("self").(*flavors.Instance)
	slip.CheckMethodArgCount(self, ":consumer-names", len(args), 0, 2)
	stream := self.Any.(jetstream.Stream)

	ctx := context.Background()
	if v, has := slip.GetArgsKeyValue(args, slip.Symbol(":timeout")); has {
		var cf context.CancelFunc
		ctx, cf = context.WithTimeout(ctx, mustBeDuration(v, ":timeout"))
		defer cf()
	}
	lister := stream.ConsumerNames(ctx)
	var list slip.List
	for name := range lister.Name() {
		list = append(list, slip.String(name))
	}
	if err := lister.Err(); err != nil {
		panic(err)
	}
	return list
}

func (caller streamConsumerNamesCaller) FuncDocs() *slip.FuncDoc {
	return &slip.FuncDoc{
		Name: ":consumer-names",
		Text: `Returns a list of consumer names.`,
		Args: []*slip.DocArg{
			{Name: "&key"},
			{
				Name: ":timeout",
				Type: "real",
				Text: `The number of seconds to wait before timing out.`,
			},
		},
		Return: "list",
	}
}
