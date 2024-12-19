// Copyright (c) 2024, Peter Ohler, All rights reserved.

package jet_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/ohler55/slip/sliptest"
)

func TestCleanupPublisherOk(t *testing.T) {
	streamName := "cleanup-publisher-test"
	js, _ := createStream(t, streamName, "test.pending")
	defer func() { _ = js.DeleteStream(context.Background(), streamName) }()

	(&sliptest.Function{
		Source: fmt.Sprintf(`(let* ((js (jet-connect :url %q :user "u1" :password "password"))
                                    (future (send js :publish-async "hello" "test.pending")))
                              (jet-cleanup-publisher js)
                              (send js :close)
                              nil)`, natsURL),
		Expect: `nil`,
	}).Test(t)
}
