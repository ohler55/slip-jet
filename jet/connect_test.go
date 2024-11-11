// Copyright (c) 2024, Peter Ohler, All rights reserved.

package jet_test

import (
	"fmt"
	"testing"

	"github.com/ohler55/slip/sliptest"
)

func TestClientConnectBasic(t *testing.T) {
	(&sliptest.Function{
		Source: fmt.Sprintf(`(let ((js (jet-connect :url %q)))
                              (send js :close)
                              js)`, natsURL),
		Expect: "/#<jet-client [0-9a-f]+>/",
	}).Test(t)
}

func TestClientConnectClosedHandler(t *testing.T) {
	(&sliptest.Function{
		Source: fmt.Sprintf(`(let* ((cc (make-channel 2))
                                    (js (jet-connect :url %q :closed-callback (lambda (c) (channel-push cc 'closed))))
                                    popped)
                              (send js :close)
                              (setq popped (channel-pop cc))
                              (channel-close cc)
                              popped)`, natsURL),
		Expect: "closed",
	}).Test(t)
}
