// Copyright (c) 2025, Peter Ohler, All rights reserved.

package jet_test

import (
	"fmt"
	"testing"

	"github.com/ohler55/slip"
	"github.com/ohler55/slip/sliptest"
)

func TestStreamCreateOrUpdatePushConsumerOk(t *testing.T) {
	defer cleanupTestStream("create-or-update-test")

	(&sliptest.Function{
		Source: fmt.Sprintf(`(let* ((js (jet-connect :url %q :user "u1" :password "password"))
                                    (jss (send js :create-or-update-stream "create-or-update-test"
                                                  :subjects '("test.create-or-update.>")))
                                    (consumer (send jss :create-or-update-push-consumer
                                                        :name "puss"
                                                        :deliver-subject "test.puss"
                                                        :timeout 0.1))
                                    (name (send (send consumer :info) :name)))
                              (send js :close)
                              (list name consumer))`, natsURL),
		Expect: `/("puss" #<jet-push-consumer [0-9a-f]+>)/`,
	}).Test(t)
}

func TestStreamCreateOrUpdatePushConsumerError(t *testing.T) {
	defer cleanupTestStream("create-or-update-test")
	(&sliptest.Function{
		Source: fmt.Sprintf(`(let* ((js (jet-connect :url %q :user "u1" :password "password"))
                                    (jss (send js :create-or-update-stream "create-or-update-test"
                                                  :subjects '("test.create-or-update.>"))))
                              (send jss :create-or-update-push-consumer :name "create-or-update bad"))`, natsURL),
		PanicType: slip.ErrorSymbol,
	}).Test(t)
}
