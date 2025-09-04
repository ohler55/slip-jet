// Copyright (c) 2025, Peter Ohler, All rights reserved.

package jet_test

import (
	"fmt"
	"testing"

	"github.com/ohler55/slip"
	"github.com/ohler55/slip/sliptest"
)

func TestClientCreatePushConsumerOk(t *testing.T) {
	defer cleanupTestStream("create-test")

	(&sliptest.Function{
		Source: fmt.Sprintf(`(let* ((js (jet-connect :url %q :user "u1" :password "password"))
                                    (jss (send js :create-stream "create-test" :subjects '("test.create.>")))
                                    (consumer (send js :create-push-consumer "create-test"
                                                       :deliver-subject "test.quux"
                                                       :name "puss"
                                                       :timeout 0.1))
                                    (name (send (send consumer :info) :name)))
                              (send js :close)
                              (list name consumer))`, natsURL),
		Expect: `/("puss" #<jet-push-consumer [0-9a-f]+>)/`,
	}).Test(t)
}

func TestClientCreatePushConsumerError(t *testing.T) {
	defer cleanupTestStream("create-test")

	(&sliptest.Function{
		Source: fmt.Sprintf(`(let* ((js (jet-connect :url %q :user "u1" :password "password"))
                                    (jss (send js :create-stream "create-test" :subjects '("test.create.>"))))
                               (recover r (progn (send js :close) (panic r))
                                 (send js :create-push-consumer "create bad")))`, natsURL),
		PanicType: slip.ErrorSymbol,
	}).Test(t)
}
