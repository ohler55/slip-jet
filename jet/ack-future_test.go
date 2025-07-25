// Copyright (c) 2024, Peter Ohler, All rights reserved.

package jet_test

import (
	"fmt"
	"testing"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/ohler55/ojg/tt"
	"github.com/ohler55/slip"
	"github.com/ohler55/slip-jet/jet"
	"github.com/ohler55/slip/sliptest"
)

// TBD uncomment test once ack-future has vanilla methods or some of them anyway

// func TestAckFutureDescribe(t *testing.T) {
// 	var out strings.Builder
// 	scope := slip.NewScope()
// 	scope.Let(slip.Symbol("out"), &slip.OutputStream{Writer: &out})

// 	scope.Let("af", jet.MakeAckFuture(nil))
// 	(&sliptest.Function{
// 		Scope:  scope,
// 		Source: `(send af :describe out)`,
// 		Expect: "nil",
// 	}).Test(t)
// 	tt.Equal(t, "/an instance of .*jet-ack-future/", out.String())

// 	out.Reset()
// 	(&sliptest.Function{
// 		Scope:  scope,
// 		Source: `(describe (find-class 'jet-ack-future) out)`,
// 		Expect: "",
// 	}).Test(t)
// 	tt.Equal(t, "/jet-ack-future.* is a built-in class/", out.String())

// 	out.Reset()
// 	(&sliptest.Function{
// 		Scope:  scope,
// 		Source: `(describe-method 'jet-ack-future :result out)`,
// 		Expect: "",
// 	}).Test(t)
// 	tt.Equal(t, "/result.* is a method of .*jet-ack-future/", out.String())
// }

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
