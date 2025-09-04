// Copyright (c) 2025, Peter Ohler, All rights reserved.

package jet_test

import (
	"fmt"
	"testing"

	"github.com/ohler55/slip"
	"github.com/ohler55/slip/sliptest"
)

func TestClientCreateOrUpdatePushConsumerOk(t *testing.T) {
	defer cleanupTestStream("create-or-update-test")

	(&sliptest.Function{
		Source: fmt.Sprintf(`(let* ((js (jet-connect :url %q :user "u1" :password "password"))
                                    (jss (send js :create-stream "create-or-update-test" :subjects '("test.create.>")))
                                    (consumer (send js :create-or-update-push-consumer "create-or-update-test"
                                                       :deliver-subject "test.quux"
                                                       :name "puss"
                                                       :timeout 0.1))
                                    (name (send (send consumer :info) :name)))
                              (send js :close)
                              (list name consumer))`, natsURL),
		Expect: `/("puss" #<jet-push-consumer [0-9a-f]+>)/`,
	}).Test(t)
}

func TestClientCreateOrUpdatePushConsumerError(t *testing.T) {
	defer cleanupTestStream("create-or-update-test")

	(&sliptest.Function{
		Source: fmt.Sprintf(`(let* ((js (jet-connect :url %q :user "u1" :password "password"))
                                    (jss (send js :create-stream "create-or-update-test" :subjects '("test.create.>"))))
                               (recover r (progn (send js :close) (panic r))
                                 (send js :create-or-update-push-consumer "create bad")))`, natsURL),
		PanicType: slip.ErrorSymbol,
	}).Test(t)
}
