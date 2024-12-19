// Copyright (c) 2024, Peter Ohler, All rights reserved.

package jet_test

import (
	"fmt"
	"testing"

	"github.com/ohler55/slip"
	"github.com/ohler55/slip/sliptest"
)

func TestClientCreateConsumerOk(t *testing.T) {
	defer cleanupTestStream("create-test")

	(&sliptest.Function{
		Source: fmt.Sprintf(`(let* ((js (jet-connect :url %q :user "u1" :password "password"))
                                    (jss (send js :create-stream "create-test" :subjects '("test.create.>")))
                                    (consumer (send js :create-consumer "create-test" :timeout 0.1 :name "eater"))
                                    (name (send consumer :name)))
                              (send js :close)
                              (list name consumer))`, natsURL),
		Expect: `/("eater" #<jet-consumer [0-9a-f]+>)/`,
	}).Test(t)
}

func TestClientCreateConsumerError(t *testing.T) {
	defer cleanupTestStream("create-test")

	(&sliptest.Function{
		Source: fmt.Sprintf(`(let* ((js (jet-connect :url %q :user "u1" :password "password"))
                                    (jss (send js :create-stream "create-test" :subjects '("test.create.>"))))
                              (send js :create-consumer "create bad"))`, natsURL),
		PanicType: slip.ErrorSymbol,
	}).Test(t)
}
