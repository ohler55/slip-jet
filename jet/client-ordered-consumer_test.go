// Copyright (c) 2024, Peter Ohler, All rights reserved.

package jet_test

import (
	"fmt"
	"testing"

	"github.com/ohler55/slip"
	"github.com/ohler55/slip/sliptest"
)

func TestClientOrderedConsumerOk(t *testing.T) {
	defer cleanupTestStream("ordered-test")

	(&sliptest.Function{
		Source: fmt.Sprintf(`
(let* ((js (jet-connect :url %q :user "u1" :password "password"))
       (jss (send js :create-stream "ordered-test" :subjects '("test.ordered.>")))
       (consumer (send js :ordered-consumer "ordered-test"
                          :timeout 0.1
                          :filter-subjects '("test.ordered.one" "test.ordered.two")))
       (filters (get (send (send consumer :info :cached t) :config) :filter-subjects)))
 (send js :close)
 (list filters consumer))`, natsURL),
		Expect: `/\(\("test.ordered.one" "test.ordered.two"\) #<jet-consumer [0-9a-f]+>\)/`,
	}).Test(t)
}

func TestClientOrderedConsumerError(t *testing.T) {
	defer cleanupTestStream("ordered-test")

	(&sliptest.Function{
		Source: fmt.Sprintf(`(let* ((js (jet-connect :url %q :user "u1" :password "password"))
                                    (jss (send js :create-stream "ordered-test" :subjects '("test.ordered.>"))))
                              (send js :ordered-consumer "ordered-test" :filter-subjects '("ordered bad")))`, natsURL),
		PanicType: slip.ErrorSymbol,
	}).Test(t)
}
