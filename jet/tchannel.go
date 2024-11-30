// Copyright (c) 2024, Peter Ohler, All rights reserved.

package jet

import (
	"strconv"

	"github.com/ohler55/slip"
	"github.com/ohler55/slip/pkg/gi"
)

// TChannelSymbol is the symbol with a value of "t-channel".
const TChannelSymbol = slip.Symbol("t-channel")

// TChannel is a chan of struct{} that pops slip.True Objects.
type TChannel <-chan struct{}

// String representation of the Object.
func (obj TChannel) String() string {
	return string(obj.Append([]byte{}))
}

// Append a buffer with a representation of the Object.
func (obj TChannel) Append(b []byte) []byte {
	b = append(b, "#<t-channel "...)
	b = strconv.AppendInt(b, int64(cap(obj)), 10)
	return append(b, '>')
}

// Simplify by returning the string representation of the flavor.
func (obj TChannel) Simplify() interface{} {
	return string(obj.Append([]byte{}))
}

// Equal returns true if this Object and the other are equal in value.
func (obj TChannel) Equal(other slip.Object) (eq bool) {
	return obj == other
}

// Hierarchy returns the class hierarchy as symbols for the channel.
func (obj TChannel) Hierarchy() []slip.Symbol {
	return []slip.Symbol{TChannelSymbol, gi.ChannelSymbol, slip.TrueSymbol}
}

// Length returns the length of the object.
func (obj TChannel) Length() int {
	return len(obj)
}

// Eval returns self.
func (obj TChannel) Eval(s *slip.Scope, depth int) slip.Object {
	return obj
}

// Pop a value from a channel.
func (obj TChannel) Pop() slip.Object {
	<-obj
	return slip.True
}
