// Copyright (c) 2024, Peter Ohler, All rights reserved.

package jet_test

import (
	"fmt"
	"testing"

	"github.com/ohler55/slip"
	"github.com/ohler55/slip/sliptest"
)

func TestStreamUpdateConsumerOk(t *testing.T) {
	defer cleanupTestStream("update-test")

	(&sliptest.Function{
		Source: fmt.Sprintf(`(let* ((js (jet-connect :url %q :user "u1" :password "password"))
                                    (jss (send js :create-stream "update-test" :subjects '("test.update.>")))
                                    (consumer (send jss :create-consumer
                                                        :timeout 0.1
                                                        :name "eater"
                                                        :description "a consumer"))
                                    (consumer2 (send jss :update-consumer
                                                         :timeout 0.1
                                                         :name "eater"
                                                         :description "updated consumer"))
                                    (desc1 (get (send (send consumer :info) :config) :description))
                                    (desc2 (get (send (send consumer2 :info) :config) :description)))
                              (send js :close)
                              (list desc1 desc2))`, natsURL),
		Expect: `("updated consumer" "updated consumer")`,
	}).Test(t)
}

func TestStreamUpdateConsumerError(t *testing.T) {
	defer cleanupTestStream("update-test")

	(&sliptest.Function{
		Source: fmt.Sprintf(`(let* ((js (jet-connect :url %q :user "u1" :password "password"))
                                    (jss (send js :create-stream "update-test" :subjects '("test.update.>"))))
                              (send jss :update-consumer :name "update bad"))`, natsURL),
		PanicType: slip.ErrorSymbol,
	}).Test(t)
}
