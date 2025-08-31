// Copyright (c) 2024, Peter Ohler, All rights reserved.

package jet_test

import (
	"fmt"
	"testing"
	"time"

	"github.com/ohler55/slip"
	"github.com/ohler55/slip/sliptest"
)

func TestClientPauseConsumerOk(t *testing.T) {
	defer cleanupTestStream("pause-test")

	t.Skip()
	// TBD pause causes a hang. Try with a push consumer
	//  if non-push consumers always then detect and raise an error

	// tm := time.Date(2024, time.December, 9, 19, 00, 2, 123, time.UTC)
	tm := time.Now().Add(time.Second)
	(&sliptest.Function{
		Source: fmt.Sprintf(`(let* ((js (jet-connect :url %q :user "u1" :password "password"))
                                    (jss (send js :create-stream "pause-test" :subjects '("test.pause.>")))
                                    (consumer (send js :create-push-consumer "pause-test" :timeout 0.1 :name "eater"))
                                    result)
(format t "*** before pause\n")
                              (send js :pause-consumer "pause-test" "eater" %s :timeout 1.1)
                              (setq result (send consumer :info))
                              (send js :close)
                              result)`, natsURL, slip.Time(tm)),
		Expect: "nil",
	}).Test(t)
}

func TestClientPauseConsumerError(t *testing.T) {
	defer cleanupTestStream("pause-test")

	(&sliptest.Function{
		Source: fmt.Sprintf(`(let* ((js (jet-connect :url %q :user "u1" :password "password"))
                                    (jss (send js :create-stream "pause-test" :subjects '("test.pause.>"))))
                              (send js :pause-consumer "pause-test" "pause bad"))`, natsURL),
		PanicType: slip.ErrorSymbol,
	}).Test(t)
}
