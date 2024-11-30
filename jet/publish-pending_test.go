// Copyright (c) 2024, Peter Ohler, All rights reserved.

package jet_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/ohler55/slip"
	"github.com/ohler55/slip/sliptest"
)

func TestPublishPendingOk(t *testing.T) {
	streamName := "publish-pending-test"
	js, _ := createStream(t, streamName, "test.pending")
	defer func() { _ = js.DeleteStream(context.Background(), streamName) }()

	(&sliptest.Function{
		Source: fmt.Sprintf(`(let* ((js (jet-connect :url %q :user "u1" :password "password"))
                                    (future (send js :publish-async "hello" "test.pending"))
                                    (before (send js :publish-pending)) ;; race condition
                                    (ack (send future :result))
                                    (after (jet-publish-pending js)))
                              (send js :close)
                              (list before after))`, natsURL),
		Expect: `(1 0)`,
	}).Test(t)
}

func TestPublishPendingNotClient(t *testing.T) {
	(&sliptest.Function{
		Source: fmt.Sprintf(`(let ((js (jet-connect :url %q :user "u1" :password "password")))
                              (send js :close)
                              (send js :publish-pending))`, natsURL),
		PanicType: slip.ErrorSymbol,
	}).Test(t)
}
