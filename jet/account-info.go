// Copyright (c) 2024, Peter Ohler, All rights reserved.

package jet

import (
	"context"

	"github.com/ohler55/ojg"
	"github.com/ohler55/ojg/alt"
	"github.com/ohler55/slip"
	"github.com/ohler55/slip/pkg/bag"
	"github.com/ohler55/slip/pkg/flavors"
)

type accountInfoCaller struct{}

func (caller accountInfoCaller) Call(s *slip.Scope, args slip.List, _ int) slip.Object {
	self := s.Get("self").(*flavors.Instance)
	slip.CheckMethodArgCount(self, ":account-info", len(args), 0, 2)
	js := self.Any.(*Client).js

	ctx := context.Background()
	if v, has := slip.GetArgsKeyValue(args, slip.Symbol(":timeout")); has {
		var cf context.CancelFunc
		ctx, cf = context.WithTimeout(ctx, mustBeDuration(v, ":timeout"))
		defer cf()
	}
	ai, err := js.AccountInfo(ctx)
	if err != nil {
		panic(err)
	}
	simple := alt.Decompose(ai, &ojg.Options{CreateKey: ""})
	inst := bag.Flavor().MakeInstance().(*flavors.Instance)
	inst.Any = simple

	return inst
}

func (caller accountInfoCaller) FuncDocs() *slip.FuncDoc {
	return &slip.FuncDoc{
		Name: ":account-info",
		Text: `Returns a _bag_ with the account information as a tree of data.`,
		Args: []*slip.DocArg{
			{Name: "&key"},
			{
				Name: ":timeout",
				Type: "real",
				Text: "the number of seconds to wait before timing out.",
			},
		},
		Return: "bag",
	}
}
