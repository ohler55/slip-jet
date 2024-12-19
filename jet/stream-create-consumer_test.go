// Copyright (c) 2024, Peter Ohler, All rights reserved.

package jet_test

import (
	"fmt"
	"testing"

	"github.com/ohler55/slip"
	"github.com/ohler55/slip/sliptest"
)

func TestStreamCreateConsumerOk(t *testing.T) {
	defer cleanupTestStream("create-test")

	(&sliptest.Function{
		Source: fmt.Sprintf(`(let* ((js (jet-connect :url %q :user "u1" :password "password"))
                                    (jss (send js :create-stream "create-test" :subjects '("test.create.>")))
                                    (consumer (send jss :create-consumer :timeout 0.1 :name "eater"))
                                    (name (send consumer :name)))
                              (send js :close)
                              (list name consumer))`, natsURL),
		Expect: `/("eater" #<jet-consumer [0-9a-f]+>)/`,
	}).Test(t)
}

func TestStreamCreateConsumerError(t *testing.T) {
	defer cleanupTestStream("create-test")

	(&sliptest.Function{
		Source: fmt.Sprintf(`(let* ((js (jet-connect :url %q :user "u1" :password "password"))
                                    (jss (send js :create-stream "create-test" :subjects '("test.create.>"))))
                              (send jss :create-consumer :name "create bad"))`, natsURL),
		PanicType: slip.ErrorSymbol,
	}).Test(t)
}
