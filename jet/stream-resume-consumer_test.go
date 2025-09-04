// Copyright (c) 2025, Peter Ohler, All rights reserved.

package jet_test

import (
	"fmt"
	"testing"
	"time"

	"github.com/ohler55/slip"
	"github.com/ohler55/slip/sliptest"
)

func TestStreamResumeConsumerOk(t *testing.T) {
	defer cleanupTestStream("resume-test")

	tm := time.Now().Add(time.Minute)
	(&sliptest.Function{
		Source: fmt.Sprintf(`(let* ((js (jet-connect :url %q :user "u1" :password "password"))
                                    (jss (send js :create-stream "resume-test" :subjects '("test.resume.>")))
                                    (consumer (send jss :create-consumer
                                                        :ack-policy :all
                                                        :durable "paws"
                                                        :pause-until %s
                                                        :timeout 0.1))
                                    result)
                              (send jss :resume-consumer "paws" :timeout 1.0)
                              (setq result (send (send consumer :info) :paused))
                              (send js :close)
                              result)`, natsURL, slip.Time(tm)),
		Expect: "nil",
	}).Test(t)
}

func TestStreamResumeConsumerNotFound(t *testing.T) {
	defer cleanupTestStream("resume-test")

	(&sliptest.Function{
		Source: fmt.Sprintf(`(let* ((js (jet-connect :url %q :user "u1" :password "password"))
                                    (jss (send js :create-stream "resume-test" :subjects '("test.resume.>"))))
                               (recover r (progn (send js :close) (panic r))
                                 (send jss :resume-consumer "resume bad")))`, natsURL),
		PanicType: slip.ErrorSymbol,
	}).Test(t)
}

func TestStreamResumeConsumerError(t *testing.T) {
	defer cleanupTestStream("resume-test")

	(&sliptest.Function{
		Source: fmt.Sprintf(`(let* ((js (jet-connect :url %q :user "u1" :password "password"))
                                    (jss (send js :create-stream "resume-test" :subjects '("test.resume.>")))
                                    (consumer (send jss :create-consumer
                                                        :ack-policy :all
                                                        :durable "paws"
                                                        :timeout 0.1)))
                              (send js :close)
                              (send jss :resume-consumer "paws" :timeout 1.0))`, natsURL),
		PanicType: slip.ErrorSymbol,
	}).Test(t)
}
