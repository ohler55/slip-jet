// Copyright (c) 2024, Peter Ohler, All rights reserved.

package jet_test

import (
	"fmt"
	"strings"
	"testing"

	"github.com/ohler55/ojg/tt"
	"github.com/ohler55/slip"
	"github.com/ohler55/slip/sliptest"
)

func TestClientDocs(t *testing.T) {
	scope := slip.NewScope()
	var out strings.Builder
	scope.Let(slip.Symbol("out"), &slip.OutputStream{Writer: &out})

	for _, method := range []string{
		":init",
	} {
		_ = slip.ReadString(fmt.Sprintf(`(describe-method 'jet-client %s out)`, method)).Eval(scope, nil)
		tt.Equal(t, true, strings.Contains(out.String(), method))
		out.Reset()
	}
}

func TestClientConnectBasic(t *testing.T) {
	(&sliptest.Function{
		Source: fmt.Sprintf(`(let ((js (jet-connect :url %q)))
                              ;; (send js :close)
                              js)`, natsURL),
		Expect: "/#<jet-client [0-9a-f]+>/",
	}).Test(t)
}
