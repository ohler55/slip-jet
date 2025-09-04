// Copyright (c) 2025, Peter Ohler, All rights reserved.

package jet_test

import (
	"fmt"
	"testing"

	"github.com/ohler55/slip"
	"github.com/ohler55/slip/sliptest"
)

func TestStreamUnpinConsumerOk(t *testing.T) {
	defer cleanupTestStream("unpin-test")

	(&sliptest.Function{
		Source: fmt.Sprintf(`(let* ((js (jet-connect :url %q :user "u1" :password "password"))
                                    (jss (send js :create-stream "unpin-test" :subjects '("test.unpin.>")))
                                    (consumer (send jss :create-consumer
                                                        :ack-policy :all
                                                        :durable "pin"
                                                        :priority-groups '("pg")
                                                        :timeout 0.1)))
                              (send jss :unpin-consumer "pin" "pg" :timeout 1.0)
                              (send js :close))`, natsURL),
		Expect: "nil",
	}).Test(t)
}

func TestStreamUnpinConsumerError(t *testing.T) {
	defer cleanupTestStream("unpin-test")

	(&sliptest.Function{
		Source: fmt.Sprintf(`(let* ((js (jet-connect :url %q :user "u1" :password "password"))
                                    (jss (send js :create-stream "unpin-test" :subjects '("test.unpin.>")))
                                    (consumer (send jss :create-consumer
                                                        :ack-policy :all
                                                        :durable "pin"
                                                        :priority-groups '("pg")
                                                        :timeout 0.1)))
                              (send js :close)
                              (send jss :unpin-consumer "pin" "pg" :timeout 1.0))`, natsURL),
		PanicType: slip.ErrorSymbol,
	}).Test(t)
}
