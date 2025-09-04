// Copyright (c) 2025, Peter Ohler, All rights reserved.

package jet_test

import (
	"fmt"
	"testing"

	"github.com/ohler55/slip"
	"github.com/ohler55/slip/sliptest"
)

func TestStreamUpdatePushConsumerOk(t *testing.T) {
	defer cleanupTestStream("update-test")

	(&sliptest.Function{
		Source: fmt.Sprintf(`(let* ((js (jet-connect :url %q :user "u1" :password "password"))
                                    (jss (send js :create-stream "update-test" :subjects '("test.update.>")))
                                    (consumer (send jss :create-push-consumer
                                                        :name "puss"
                                                        :deliver-subject "test.quux"))
                                    (updated (send jss :update-push-consumer
                                                       :name "puss"
                                                       :deliver-subject "test.puss"
                                                       :timeout 0.1))
                                    (name (send (send updated :info) :name)))
                              (send js :close)
                              (send consumer :equal updated))`, natsURL),
		Expect: "t",
	}).Test(t)
}

func TestStreamUpdatePushConsumerError(t *testing.T) {
	defer cleanupTestStream("update-test")

	(&sliptest.Function{
		Source: fmt.Sprintf(`(let* ((js (jet-connect :url %q :user "u1" :password "password"))
                                    (jss (send js :create-stream "update-test" :subjects '("test.update.>"))))
                              (send jss :update-push-consumer :name "update bad"))`, natsURL),
		PanicType: slip.ErrorSymbol,
	}).Test(t)
}
