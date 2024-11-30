// Copyright (c) 2024, Peter Ohler, All rights reserved.

package jet_test

import (
	"fmt"
	"strings"
	"testing"

	"github.com/ohler55/ojg/tt"
	"github.com/ohler55/slip"
	"github.com/ohler55/slip-jet/jet"
	"github.com/ohler55/slip/pkg/flavors"
	"github.com/ohler55/slip/sliptest"
)

func TestClientDocs(t *testing.T) {
	scope := slip.NewScope()
	var out strings.Builder
	scope.Let(slip.Symbol("out"), &slip.OutputStream{Writer: &out})

	for _, method := range []string{
		":init",
		":cleanup-publisher",
		":close",
		":options",
		":publish",
		":publish-async",
		":publish-complete",
		":publish-pending",
	} {
		_ = slip.ReadString(fmt.Sprintf(`(describe-method 'jet-client %s out)`, method)).Eval(scope, nil)
		tt.Equal(t, true, strings.Contains(out.String(), method))
		out.Reset()
	}
}

func TestClientJetStream(t *testing.T) {
	scope := slip.NewScope()
	scope.Let(slip.Symbol("js"), nil)
	defer func() {
		_ = slip.ReadString("(send js :close)").Eval(scope, nil)
	}()
	(&sliptest.Function{
		Scope:  scope,
		Source: fmt.Sprintf(`(setq js (jet-connect :url %q :user "u1" :password "password"))`, natsURL),
		Expect: "/#<jet-client [0-9a-f]+>/",
	}).Test(t)
	inst, ok := scope.Get("js").(*flavors.Instance)
	tt.Equal(t, true, ok)
	js := inst.Any.(*jet.Client).JetStream()
	tt.NotNil(t, js)
}
