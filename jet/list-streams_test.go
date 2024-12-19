// Copyright (c) 2024, Peter Ohler, All rights reserved.

package jet_test

import (
	"fmt"
	"testing"

	"github.com/ohler55/slip/sliptest"
)

func TestListStreamOk(t *testing.T) {
	defer cleanupTestStream("list-test")

	(&sliptest.Function{
		Source: fmt.Sprintf(`(let* ((js (jet-connect :url %q :user "u1" :password "password"))
                                    (jss (send js :create-stream "list-test" :subjects '("test.list.>")))
                                    (info (send js :list-streams :timeout 0.1 :subject "test.list.one")))
                              (send js :close)
                              info)`, natsURL),
		Expect: `/\(#<jet-stream-info [0-9a-f]+>\)/`,
	}).Test(t)
}
