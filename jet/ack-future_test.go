// Copyright (c) 2024, Peter Ohler, All rights reserved.

package jet_test

import (
	"fmt"
	"strings"
	"testing"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/ohler55/ojg/tt"
	"github.com/ohler55/slip"
	"github.com/ohler55/slip-jet/jet"
	"github.com/ohler55/slip/sliptest"
)

type badWriter int

func (w badWriter) Write([]byte) (int, error) {
	return 0, fmt.Errorf("oops")
}

func TestAckFutureDescribe(t *testing.T) {
	var out strings.Builder
	scope := slip.NewScope()
	scope.Let(slip.Symbol("out"), &slip.OutputStream{Writer: &out})

	scope.Let("af", jet.MakeAckFuture(nil))
	(&sliptest.Function{
		Scope:  scope,
		Source: `(let ((*print-ansi* t)) (send af :describe out))`,
		Expect: "nil",
	}).Test(t)
	tt.Equal(t, "/an instance of .*jet-ack-future/", out.String())

	(&sliptest.Function{
		Scope:  scope,
		Source: `(let ((*print-ansi* nil)) (send af :describe))`,
		Expect: "nil",
	}).Test(t)
	tt.Equal(t, "/an instance of .*jet-ack-future/", out.String())

	out.Reset()
	(&sliptest.Function{
		Scope:  scope,
		Source: `(describe (find-class 'jet-ack-future) out)`,
		Expect: "",
	}).Test(t)
	tt.Equal(t, "/jet-ack-future.* is a flavor/", out.String())

	out.Reset()
	(&sliptest.Function{
		Scope:  scope,
		Source: `(describe-method 'jet-ack-future :result out)`,
		Expect: "",
	}).Test(t)
	tt.Equal(t, "/result.* is a method of .*jet-ack-future/", out.String())

	(&sliptest.Function{
		Scope:     scope,
		Source:    `(send af :describe out t)`,
		PanicType: slip.ErrorSymbol,
	}).Test(t)

	(&sliptest.Function{
		Scope:     scope,
		Source:    `(send af :describe 7)`,
		PanicType: slip.TypeErrorSymbol,
	}).Test(t)
}

func TestAckFuturePrintSelf(t *testing.T) {
	var out strings.Builder
	scope := slip.NewScope()
	scope.Let(slip.Symbol("out"), &slip.OutputStream{Writer: &out})

	scope.Let("af", jet.MakeAckFuture(nil))
	(&sliptest.Function{
		Scope:  scope,
		Source: `(let ((*standard-output* out)) (send af :print-self))`,
		Expect: "nil",
	}).Test(t)
	tt.Equal(t, "/#<jet-ack-future [0-9a-f]+>/", out.String())

	out.Reset()
	(&sliptest.Function{
		Scope:  scope,
		Source: `(send af :print-self out)`,
		Expect: "nil",
	}).Test(t)
	tt.Equal(t, "/#<jet-ack-future [0-9a-f]+>/", out.String())

	(&sliptest.Function{
		Scope:     scope,
		Source:    `(send af :print-self 7)`,
		PanicType: slip.TypeErrorSymbol,
	}).Test(t)

	scope.Let(slip.Symbol("bad"), &slip.OutputStream{Writer: badWriter(0)})
	(&sliptest.Function{
		Scope:     scope,
		Source:    `(send af :print-self bad)`,
		PanicType: slip.StreamErrorSymbol,
	}).Test(t)
}

func TestAckFutureID(t *testing.T) {
	scope := slip.NewScope()
	scope.Let("af", jet.MakeAckFuture(nil))
	(&sliptest.Function{
		Scope:  scope,
		Source: `(send af :id)`,
		Expect: "/[0-9]+/",
	}).Test(t)
}

func TestAckFutureWhichOperations(t *testing.T) {
	scope := slip.NewScope()
	scope.Let("af", jet.MakeAckFuture(nil))
	(&sliptest.Function{
		Scope:  scope,
		Source: `(send af :which-operations)`,
		Expect: `(:change-class :change-flavor :describe :equal :eval-inside-yourself :flavor :id
               :inspect :message :operation-handled-p :print-self :result
               :send-if-handles :shared-initialize
               :update-instance-for-different-class :which-operations)`,
	}).Test(t)
}

func TestAckFutureClass(t *testing.T) {
	scope := slip.NewScope()
	scope.Let("af", jet.MakeAckFuture(nil))
	(&sliptest.Function{
		Scope:  scope,
		Source: `(send af :class)`,
		Expect: "#<flavor jet-ack-future>",
	}).Test(t)
}

func TestAckFutureOperationHandledP(t *testing.T) {
	scope := slip.NewScope()
	scope.Let("af", jet.MakeAckFuture(nil))
	(&sliptest.Function{
		Scope:  scope,
		Source: `(send af :operation-handled-p :flavor)`,
		Expect: "t",
	}).Test(t)
	(&sliptest.Function{
		Scope:  scope,
		Source: `(send af :operation-handled-p :quux)`,
		Expect: "nil",
	}).Test(t)
}

func TestAckFutureEqual(t *testing.T) {
	scope := slip.NewScope()
	scope.Let("af", jet.MakeAckFuture(nil))
	scope.Let("af2", jet.MakeAckFuture(nil))
	(&sliptest.Function{
		Scope:  scope,
		Source: `(send af :equal af)`,
		Expect: "t",
	}).Test(t)
	(&sliptest.Function{
		Scope:  scope,
		Source: `(send af :equal af2)`,
		Expect: "nil",
	}).Test(t)
}

func TestAckFutureChangeClass(t *testing.T) {
	scope := slip.NewScope()
	scope.Let("af", jet.MakeAckFuture(nil))
	(&sliptest.Function{
		Scope:     scope,
		Source:    `(send af :change-class)`,
		PanicType: slip.ErrorSymbol,
	}).Test(t)
}

func TestAckFutureEvalInsideSelf(t *testing.T) {
	scope := slip.NewScope()
	scope.Let("af", jet.MakeAckFuture(nil))
	(&sliptest.Function{
		Scope:  scope,
		Source: `(send af :eval-inside-yourself '(+ 1 2))`,
		Expect: "3",
	}).Test(t)
	(&sliptest.Function{
		Scope:     scope,
		Source:    `(send af :eval-inside-yourself '(+ 1 2) t)`,
		PanicType: slip.ErrorSymbol,
	}).Test(t)
}

func TestAckFutureInspect(t *testing.T) {
	scope := slip.NewScope()
	scope.Let("af", jet.MakeAckFuture(nil))
	(&sliptest.Function{
		Scope:  scope,
		Source: `(send af :inspect) `,
		Expect: "/#<bag-flavor [0-9a-f]+>/",
	}).Test(t)
}

func TestAckFutureSendIfHandles(t *testing.T) {
	scope := slip.NewScope()
	scope.Let("af", jet.MakeAckFuture(nil))
	(&sliptest.Function{
		Scope:  scope,
		Source: `(send af :send-if-handles :flavor) `,
		Expect: "#<flavor jet-ack-future>",
	}).Test(t)
	(&sliptest.Function{
		Scope:  scope,
		Source: `(send af :send-if-handles :quux) `,
		Expect: "nil",
	}).Test(t)
	(&sliptest.Function{
		Scope:     scope,
		Source:    `(send af :send-if-handles) `,
		PanicType: slip.ErrorSymbol,
	}).Test(t)
}

func TestAckFutureMisc(t *testing.T) {
	af := jet.MakeAckFuture(nil)
	tt.Equal(t, true, af.IsA("jet-ack-future"))
	tt.Equal(t, false, af.IsA("jet-ack"))
	af.SetSynchronized(false)
	tt.Equal(t, false, af.Synchronized())
	tt.Equal(t, []string{}, af.SlotNames())
	_, has := af.SlotValue("quux")
	tt.Equal(t, false, has)
	tt.Equal(t, false, af.SetSlotValue(slip.Symbol("quux"), nil))
	tt.Nil(t, af.GetMethod(":quux"))
	tt.Nil(t, af.Dup())
}

func TestAckFutureChangeNoMethod(t *testing.T) {
	scope := slip.NewScope()
	scope.Let("af", jet.MakeAckFuture(nil))
	(&sliptest.Function{
		Scope:     scope,
		Source:    `(send af :quux)`,
		PanicType: slip.NoApplicableMethodErrorSymbol,
	}).Test(t)
}

func TestAckFutureObject(t *testing.T) {
	af := jet.MakeAckFuture(nil)
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

func TestAckFutureResult(t *testing.T) {
	scope := slip.NewScope()
	jaf := mockAckFuture{
		okChan:  make(chan *jetstream.PubAck, 1),
		errChan: make(chan error, 1),
		msg:     nil,
	}
	scope.Let("af", jet.MakeAckFuture(&jaf))
	jaf.okChan <- &jetstream.PubAck{}
	(&sliptest.Function{
		Scope:  scope,
		Source: `(send af :result)`,
		Expect: "/#<jet-ack [0-9a-f]+>/",
	}).Test(t)

	jaf.errChan <- fmt.Errorf("broke")
	(&sliptest.Function{
		Scope:  scope,
		Source: `(send af :result)`,
		Expect: "/#<error [0-9a-f]+>/",
	}).Test(t)
}

func TestAckFutureMessage(t *testing.T) {
	scope := slip.NewScope()
	af := mockAckFuture{
		msg: &nats.Msg{
			Data:    []byte("hello"),
			Subject: "test.sub",
		},
	}
	scope.Let("af", jet.MakeAckFuture(&af))
	(&sliptest.Function{
		Scope:  scope,
		Source: `(send af :message)`,
		Expect: "/#<jet-msg [0-9a-f]+>/",
	}).Test(t)
}

func TestDocCaller(t *testing.T) {
	tt.Equal(t, "test", (&jet.DocCaller{Text: "test"}).Docs())
	tt.Nil(t, (&jet.DocCaller{Text: "test"}).Call(nil, nil, 0))
}
