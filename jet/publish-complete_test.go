// Copyright (c) 2024, Peter Ohler, All rights reserved.

package jet_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/ohler55/slip"
	"github.com/ohler55/slip/sliptest"
)

func TestPublishCompleteOk(t *testing.T) {
	streamName := "publish-complete-test"
	js, _ := createStream(t, streamName, "test.complete")
	defer func() { _ = js.DeleteStream(context.Background(), streamName) }()

	(&sliptest.Function{
		Source: fmt.Sprintf(`(let* ((js (jet-connect :url %q :user "u1" :password "password"))
                                    (future (send js :publish-async "hello" "test.complete"))
                                    (complete (channel-pop (send js :publish-complete))))
                              (send js :close)
                              complete)`, natsURL),
		Expect: `t`,
	}).Test(t)
}

func TestPublishCompleteNotClient(t *testing.T) {
	(&sliptest.Function{
		Source: fmt.Sprintf(`(let ((js (jet-connect :url %q :user "u1" :password "password")))
                              (send js :close)
                              (send js :publish-complete))`, natsURL),
		PanicType: slip.ErrorSymbol,
	}).Test(t)
}
