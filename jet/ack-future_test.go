// Copyright (c) 2024, Peter Ohler, All rights reserved.

package jet_test

import (
	"strings"
	"testing"

	"github.com/ohler55/ojg/tt"
	"github.com/ohler55/slip"
	"github.com/ohler55/slip-jet/jet"
	"github.com/ohler55/slip/sliptest"
)

func TestAckFutureDescribe(t *testing.T) {
	var out strings.Builder
	scope := slip.NewScope()
	scope.Let(slip.Symbol("out"), &slip.OutputStream{Writer: &out})

	scope.Let("af", jet.MakeAckFuture(nil, nil))
	(&sliptest.Function{
		Scope:  scope,
		Source: `(send af :describe out)`,
		Expect: "nil",
	}).Test(t)
	tt.Equal(t, "/an instance of .*jet-ack-future/", out.String())

	out.Reset()
	(&sliptest.Function{
		Scope:  scope,
		Source: `(describe (find-class 'jet-ack-future) out)`,
		Expect: "",
	}).Test(t)
	tt.Equal(t, "/jet-ack-future.* is a built-in class/", out.String())

	out.Reset()
	(&sliptest.Function{
		Scope:  scope,
		Source: `(describe-method 'jet-ack-future :result out)`,
		Expect: "",
	}).Test(t)
	tt.Equal(t, "/result.* is a method of .*jet-ack-future/", out.String())
}

func TestAckFutureObject(t *testing.T) {
	af := jet.MakeAckFuture(nil, nil)
	(&sliptest.Object{
		Target:    af,
		String:    "/#<jet-ack-future [0-9a-f]+>/",
		Simple:    af.String(),
		Hierarchy: "jet-ack-future.t",
		Equals: []*sliptest.EqTest{
			{Other: af, Expect: true},
			{Other: slip.Fixnum(5), Expect: false},
		},
		Eval: af,
	}).Test(t)

	tt.Equal(t, "jet-ack-future", af.Class().Name())
	tt.Equal(t, true, af.HasMethod(":result"))
	tt.Equal(t, true, af.HasMethod(":id"))
	tt.Equal(t, false, af.HasMethod(":nothing"))
}

func TestAckFutureresult(t *testing.T) {
	scope := slip.NewScope()
	scope.Let("af", jet.MakeAckFuture(nil, nil))
	(&sliptest.Function{
		Scope:  scope,
		Source: `(send af :result)`,
		Expect: "nil",
	}).Test(t)
}

func TestAckFutureMessage(t *testing.T) {
	scope := slip.NewScope()
	scope.Let("af", jet.MakeAckFuture(nil, nil))
	(&sliptest.Function{
		Scope:  scope,
		Source: `(send af :message)`,
		Expect: "nil",
	}).Test(t)
}

func TestDocCaller(t *testing.T) {
	tt.Equal(t, "test", (&jet.DocCaller{Text: "test"}).Docs())
}
