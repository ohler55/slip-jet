// Copyright (c) 2024, Peter Ohler, All rights reserved.

package jet_test

import (
	"testing"

	"github.com/ohler55/ojg/tt"
	"github.com/ohler55/slip-jet/jet"
)

func TestAckMake(t *testing.T) {
	ack := jet.MakeAck("river", 123, true, "domino")
	tt.NotNil(t, ack)
}
