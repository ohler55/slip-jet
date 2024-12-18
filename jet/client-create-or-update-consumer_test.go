// Copyright (c) 2024, Peter Ohler, All rights reserved.

package jet_test

import (
	"fmt"
	"testing"

	"github.com/ohler55/slip"
	"github.com/ohler55/slip/sliptest"
)

func TestClientCreateOrUpdateConsumerOk(t *testing.T) {
	defer cleanupTestStream("create-or-update-test")

	(&sliptest.Function{
		Source: fmt.Sprintf(`
(let* ((js (jet-connect :url %q :user "u1" :password "password"))
       (jss (send js :create-stream "create-or-update-test" :subjects '("test.update.>")))
       (consumer (send js :create-or-update-consumer "create-or-update-test" :name "eater"))
       (updated (send js :create-or-update-consumer "create-or-update-test" :timeout 0.1 :name "eater")))
 (send js :close)
 (send consumer :equal updated))`, natsURL),
		Expect: "t",
	}).Test(t)
}

func TestClientCreateOrUpdateConsumerError(t *testing.T) {
	defer cleanupTestStream("create-or-update-test")

	(&sliptest.Function{
		Source: fmt.Sprintf(`(let* ((js (jet-connect :url %q :user "u1" :password "password"))
                                    (jss (send js :create-stream "create-or-update-test"
                                                  :subjects '("test.update.>"))))
                              (send js :create-or-update-consumer "update bad"))`, natsURL),
		PanicType: slip.ErrorSymbol,
	}).Test(t)
}
