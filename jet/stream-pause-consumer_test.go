// Copyright (c) 2025, Peter Ohler, All rights reserved.

package jet_test

import (
	"fmt"
	"testing"
	"time"

	"github.com/ohler55/slip"
	"github.com/ohler55/slip/sliptest"
)

func TestStreamPauseConsumerOk(t *testing.T) {
	defer cleanupTestStream("pause-test")

	tm := time.Now().Add(time.Minute)
	(&sliptest.Function{
		Source: fmt.Sprintf(`(let* ((js (jet-connect :url %q :user "u1" :password "password"))
                                    (jss (send js :create-stream "pause-test" :subjects '("test.pause.>")))
                                    (consumer (send jss :create-consumer
                                                        :ack-policy :all
                                                        :durable "paws"
                                                        :timeout 0.1))
                                    result)
                              (send jss :pause-consumer "paws" %s :timeout 1.0)
                              (setq result (send (send consumer :info) :paused))
                              (send js :close)
                              result)`, natsURL, slip.Time(tm)),
		Expect: "t",
	}).Test(t)
}

func TestStreamPauseConsumerNotFound(t *testing.T) {
	defer cleanupTestStream("pause-test")

	tm := time.Now().Add(time.Minute)
	(&sliptest.Function{
		Source: fmt.Sprintf(`(let* ((js (jet-connect :url %q :user "u1" :password "password"))
                                    (jss (send js :create-stream "pause-test" :subjects '("test.pause.>"))))
                               (recover r (progn (send js :close) (panic r))
                                 (send jss :pause-consumer "pause bad" %s)))`, natsURL, slip.Time(tm)),
		PanicType: slip.ErrorSymbol,
	}).Test(t)
}

func TestStreamPauseConsumerNotTime(t *testing.T) {
	defer cleanupTestStream("pause-test")

	(&sliptest.Function{
		Source: fmt.Sprintf(`(let* ((js (jet-connect :url %q :user "u1" :password "password"))
                                    (jss (send js :create-stream "pause-test" :subjects '("test.pause.>")))
                                    (consumer (send jss :create-consumer
                                                        :ack-policy :all
                                                        :durable "paws"
                                                        :timeout 0.1)))
                               (recover r (progn (send js :close) (panic r))
                                 (send jss :pause-consumer "paws" t)))`, natsURL),
		PanicType: slip.TypeErrorSymbol,
	}).Test(t)
}

func TestStreamPauseConsumerError(t *testing.T) {
	defer cleanupTestStream("pause-test")

	tm := time.Now().Add(time.Minute)
	(&sliptest.Function{
		Source: fmt.Sprintf(`(let* ((js (jet-connect :url %q :user "u1" :password "password"))
                                    (jss (send js :create-stream "pause-test" :subjects '("test.pause.>")))
                                    (consumer (send jss :create-consumer
                                                        :ack-policy :all
                                                        :durable "paws"
                                                        :timeout 0.1)))
                              (send js :close)
                              (send jss :pause-consumer "paws" %s :timeout 1.0))`, natsURL, slip.Time(tm)),
		PanicType: slip.ErrorSymbol,
	}).Test(t)
}
