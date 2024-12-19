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
	flavors.CheckMethodArgCount(self, ":account-info", len(args), 0, 2)
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

func (caller accountInfoCaller) Docs() string {
	return `__:account-info__ &key _timeout_ => _bag__
   _:timeout_ [real] the number of seconds to wait before timing out.


Returns a _bag_ with the account information as a tree of data.
`
}
