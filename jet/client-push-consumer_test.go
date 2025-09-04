// Copyright (c) 2025, Peter Ohler, All rights reserved.

package jet_test

import (
	"fmt"
	"testing"

	"github.com/ohler55/slip"
	"github.com/ohler55/slip/sliptest"
)

func TestClientPushConsumerOk(t *testing.T) {
	defer cleanupTestStream("get-test")

	(&sliptest.Function{
		Source: fmt.Sprintf(`(let* ((js (jet-connect :url %q :user "u1" :password "password"))
                                    (jss (send js :create-stream "get-test" :subjects '("test.get.>")))
                                    (consumer (send jss :create-push-consumer
                                                        :name "puss"
                                                        :deliver-subject "test.puss"
                                                        :timeout 0.1))
                                    (found (send js :push-consumer "get-test" "puss" :timeout 0.1))
                                    (not-found (send js :push-consumer "get-test" "no-one")))
                              (send js :close)
                              (list not-found
                                    (send consumer :equal found)
                                    (when found (send (send found :info :cached t) :name))))`, natsURL),
		Expect: `(nil t "puss")`,
	}).Test(t)
}

func TestClientPushConsumerError(t *testing.T) {
	defer cleanupTestStream("get-test")

	(&sliptest.Function{
		Source: fmt.Sprintf(`(let* ((js (jet-connect :url %q :user "u1" :password "password"))
                                    (jss (send js :create-stream "get-test" :subjects '("test.get.>"))))
                              (send js :push-consumer "get-test" "get bad"))`, natsURL),
		PanicType: slip.ErrorSymbol,
	}).Test(t)
}
