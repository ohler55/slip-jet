// Copyright (c) 2024, Peter Ohler, All rights reserved.

package jet_test

import (
	"fmt"
	"testing"

	"github.com/ohler55/slip"
	"github.com/ohler55/slip/sliptest"
)

func TestStreamCreateOrUpdateConsumerOk(t *testing.T) {
	defer cleanupTestStream("create-or-update-test")

	(&sliptest.Function{
		Source: fmt.Sprintf(`(let* ((js (jet-connect :url %q :user "u1" :password "password"))
                                    (jss (send js :create-or-update-stream "create-or-update-test"
                                                  :subjects '("test.create-or-update.>")))
                                    (consumer (send jss :create-or-update-consumer :timeout 0.1 :name "eater"))
                                    (name (send consumer :name)))
                              (send js :close)
                              (list name consumer))`, natsURL),
		Expect: `/("eater" #<jet-consumer [0-9a-f]+>)/`,
	}).Test(t)
}

func TestStreamCreateOrUpdateConsumerError(t *testing.T) {
	defer cleanupTestStream("create-or-update-test")
	(&sliptest.Function{
		Source: fmt.Sprintf(`(let* ((js (jet-connect :url %q :user "u1" :password "password"))
                                    (jss (send js :create-or-update-stream "create-or-update-test" :subjects '("test.create-or-update.>"))))
                              (send jss :create-or-update-consumer :name "create-or-update bad"))`, natsURL),
		PanicType: slip.ErrorSymbol,
	}).Test(t)
}
