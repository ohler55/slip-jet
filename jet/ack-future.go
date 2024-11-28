// Copyright (c) 2024, Peter Ohler, All rights reserved.

package jet

import (
	"strconv"
	"strings"
	"unsafe"

	"github.com/nats-io/nats.go/jetstream"
	"github.com/ohler55/slip"
	"github.com/ohler55/slip/pkg/clos"
)

// JetAckFutureSymbol is the symbol with a value of "time-channel".
const JetAckFutureSymbol = slip.Symbol("jet-ack-future")

var ackFutureClass slip.Class

func init() {
	ackFutureClass = clos.DefClass(
		"jet-ack-future",
		`jet-ack-future is a future for a jet-ack. It can be used to wait for a
jet-ack or an error after an async publish.`,
		nil, // slots
		nil, // supers
		true,
	)
	ackFutureClass.(*clos.Class).SetNoMake(true)
	ackFutureClass.(*clos.Class).DefMethod(":result", "", &DocCaller{
		Text: `__:result__ => _channel_


Returns a channel that will either return a _jet-ack_ or and error.
`})
	ackFutureClass.(*clos.Class).DefMethod(":message", "", &DocCaller{
		Text: `__:message__ => _jet-msg_


Returns the message sent to teh server.
`})
}

// JetAckFuture is a chan of time.Time that pops slip.Time Objects.
type JetAckFuture struct {
	Ack     jetstream.PubAckFuture
	Message slip.Object
}

// String representation of the Object.
func (obj *JetAckFuture) String() string {
	return string(obj.Append([]byte{}))
}

// Append a buffer with a representation of the Object.
func (obj *JetAckFuture) Append(b []byte) []byte {
	b = append(b, "#<jet-ack-future "...)
	b = strconv.AppendUint(b, uint64(uintptr(unsafe.Pointer(obj))), 16)
	return append(b, '>')
}

// Simplify by returning the string representation of the flavor.
func (obj *JetAckFuture) Simplify() interface{} {
	return string(obj.Append([]byte{}))
}

// Equal returns true if this Object and the other are equal in value.
func (obj *JetAckFuture) Equal(other slip.Object) (eq bool) {
	return obj == other
}

// Hierarchy returns the class hierarchy as symbols for the channel.
func (obj *JetAckFuture) Hierarchy() []slip.Symbol {
	return []slip.Symbol{JetAckFutureSymbol, slip.TrueSymbol}
}

// Eval returns self.
func (obj *JetAckFuture) Eval(s *slip.Scope, depth int) slip.Object {
	return obj
}

// Pop a value from a channel.
func (obj *JetAckFuture) Pop() (result slip.Object) {
	if obj.Ack != nil {
		select {
		case pa := <-obj.Ack.Ok():
			result = makeAck(pa.Stream, pa.Sequence, pa.Duplicate, pa.Domain)
		case err := <-obj.Ack.Err():
			result = slip.NewError("%s", err)
		}
	}
	return
}

// Class of the instance.
func (obj *JetAckFuture) Class() slip.Class {
	return ackFutureClass
}

// Init does nothing.
func (obj *JetAckFuture) Init(scope *slip.Scope, args slip.List, depth int) {
}

// Receive a method invocation from the send function. It is typically
// called by the send function but can be called directly so effectively
// send a method to an instance.
func (obj *JetAckFuture) Receive(s *slip.Scope, message string, args slip.List, depth int) (result slip.Object) {
	message = strings.ToLower(message)
	switch message {
	case ":result":
		result = obj.Pop()
	case ":message":
		result = obj.Message
	default:
		result = ackFutureClass.(*clos.Class).InvokeMethod(obj, s, message, args, depth)
	}
	return
}

// HasMethod returns true if the instance handles the named method.
func (obj *JetAckFuture) HasMethod(method string) bool {
	switch strings.ToLower(method) {
	case ":result", ":message":
		return true
	}
	return ackFutureClass.(*clos.Class).GetMethod(method) != nil
}

// MakeAckFuture makes a new jet-ack-future.
func MakeAckFuture(af jetstream.PubAckFuture, message slip.Object) *JetAckFuture {
	return &JetAckFuture{
		Ack:     af,
		Message: message,
	}
}
