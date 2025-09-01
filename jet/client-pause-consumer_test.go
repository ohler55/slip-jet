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

	// t.Skip()
	// TBD pause causes a hang. Try with a push consumer
	//  if non-push consumers always then detect and raise an error

	tm := time.Now().Add(time.Minute)
	(&sliptest.Function{
		Source: fmt.Sprintf(`(let* ((js (jet-connect :url %q :user "u1" :password "password"))
                                    (jss (send js :create-stream "pause-test" :subjects '("test.pause.>")))
                                    (consumer (send js :create-consumer "pause-test"
                                                                        :ack-policy :all
                                                                        :description "is this needed?"
                                                                        :durable "paws"
                                                                        :timeout 0.1))
                                    result)
                              (send js :pause-consumer "pause-test" "paws" %s :timeout 1.0)
                              (setq result (send (send consumer :info) :paused))
                              (send js :close)
                              result)`, natsURL, slip.Time(tm)),
		Expect: "t",
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
