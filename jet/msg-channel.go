// Copyright (c) 2024, Peter Ohler, All rights reserved.

package jet

import (
	"strconv"

	"github.com/nats-io/nats.go/jetstream"
	"github.com/ohler55/slip"
	"github.com/ohler55/slip/pkg/gi"
)

// MsgChannelSymbol is the symbol with a value of "msg-channel".
const MsgChannelSymbol = slip.Symbol("jet-msg-channel")

// MsgChannel is a chan of struct{} that pops slip.True Objects.
type MsgChannel <-chan jetstream.Msg

// String representation of the Object.
func (obj MsgChannel) String() string {
	return string(obj.Append([]byte{}))
}

// Append a buffer with a representation of the Object.
func (obj MsgChannel) Append(b []byte) []byte {
	b = append(b, "#<jet-msg-channel "...)
	b = strconv.AppendInt(b, int64(cap(obj)), 10)
	return append(b, '>')
}

// Simplify by returning the string representation of the flavor.
func (obj MsgChannel) Simplify() interface{} {
	return string(obj.Append([]byte{}))
}

// Equal returns true if this Object and the other are equal in value.
func (obj MsgChannel) Equal(other slip.Object) (eq bool) {
	return obj == other
}

// Hierarchy returns the class hierarchy as symbols for the channel.
func (obj MsgChannel) Hierarchy() []slip.Symbol {
	return []slip.Symbol{MsgChannelSymbol, gi.ChannelSymbol, slip.TrueSymbol}
}

// Length returns the length of the object.
func (obj MsgChannel) Length() int {
	return len(obj)
}

// Eval returns self.
func (obj MsgChannel) Eval(s *slip.Scope, depth int) slip.Object {
	return obj
}

// Pop a value from a channel.
func (obj MsgChannel) Pop() (result slip.Object) {
	if m := <-obj; m != nil {
		result = MakeMsg(m)
	}
	return
}
