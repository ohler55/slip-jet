// Copyright (c) 2024, Peter Ohler, All rights reserved.

package jet

import (
	"io"
	"strconv"
	"strings"
	"unsafe"

	"github.com/nats-io/nats.go/jetstream"
	"github.com/ohler55/slip"
	"github.com/ohler55/slip/pkg/flavors"
)

const (
	// JetAckFutureSymbol is the symbol with a value of "jet-ack-future".
	JetAckFutureSymbol = slip.Symbol("jet-ack-future")

	bold     = "\x1b[1m"
	colorOff = "\x1b[m"
)

var ackFutureFlavor *flavors.Flavor

func defAckFuture() {
	ackFutureFlavor = flavors.DefFlavor(
		"jet-ack-future",
		map[string]slip.Object{}, // variables
		nil,                      // inherit
		slip.List{
			slip.List{
				slip.Symbol(":documentation"),
				slip.String(`jet-ack-future is a future for a jet-ack. It can be used to wait for a
jet-ack or an error after an async publish.`),
			},
		},
		&Pkg)

	ackFutureFlavor.DefMethod(":result", "", &DocCaller{
		Text: `__:result__ => _channel_


Returns a channel that will either return a _jet-ack_ or and error.
`})
	ackFutureFlavor.DefMethod(":message", "", &DocCaller{
		Text: `__:message__ => _jet-msg_


Returns the message sent to the server.
`})
}

// AckFuture is a chan of time.Time that pops slip.Time Objects.
type AckFuture struct {
	Ack jetstream.PubAckFuture
}

// String representation of the Object.
func (obj *AckFuture) String() string {
	return string(obj.Append([]byte{}))
}

// Append a buffer with a representation of the Object.
func (obj *AckFuture) Append(b []byte) []byte {
	b = append(b, "#<jet-ack-future "...)
	b = strconv.AppendUint(b, uint64(uintptr(unsafe.Pointer(obj))), 16)
	return append(b, '>')
}

// Simplify by returning the string representation of the flavor.
func (obj *AckFuture) Simplify() interface{} {
	return string(obj.Append([]byte{}))
}

// Equal returns true if this Object and the other are equal in value.
func (obj *AckFuture) Equal(other slip.Object) (eq bool) {
	return obj == other
}

// Hierarchy returns the class hierarchy as symbols for the channel.
func (obj *AckFuture) Hierarchy() []slip.Symbol {
	return []slip.Symbol{JetAckFutureSymbol, slip.TrueSymbol}
}

// Eval returns self.
func (obj *AckFuture) Eval(s *slip.Scope, depth int) slip.Object {
	return obj
}

// Pop a value from a channel.
func (obj *AckFuture) Pop() (result slip.Object) {
	if obj.Ack != nil {
		select {
		case pa := <-obj.Ack.Ok():
			result = MakeAck(pa.Stream, pa.Sequence, pa.Duplicate, pa.Domain)
		case err := <-obj.Ack.Err():
			result = slip.ErrorNew(slip.NewScope(), 0, "%s", err)
		}
	}
	return
}

// Class of the instance.
func (obj *AckFuture) Class() slip.Class {
	return ackFutureFlavor
}

// Init does nothing.
func (obj *AckFuture) Init(scope *slip.Scope, args slip.List, depth int) {
}

// ID returns unique ID for the instance.
func (obj *AckFuture) ID() uint64 {
	return uint64(uintptr(unsafe.Pointer(obj)))
}

// Dup returns a duplicate of the instance.
func (obj *AckFuture) Dup() slip.Instance {
	return nil
}

// IsA returns true if the instance's class is the specified class or a
// sub-class of the specified class.
func (obj *AckFuture) IsA(class string) bool {
	return class == "jet-ack-future"
}

// SetSynchronized sets the instance to be thread safe (mutex protected
// slots) or not.
func (obj *AckFuture) SetSynchronized(on bool) {
}

// Synchronized returns true if the instance is thread safe.
func (obj *AckFuture) Synchronized() bool {
	return false
}

// SlotNames returns a list of the slots names for the instance.
func (obj *AckFuture) SlotNames() []string {
	return []string{}
}

// SlotValue return the value of an instance variable.
func (obj *AckFuture) SlotValue(name slip.Symbol) (slip.Object, bool) {
	return nil, false
}

// SetSlotValue sets the value of an instance variable and return true if
// the name slot exists and was set.
func (obj *AckFuture) SetSlotValue(sym slip.Symbol, value slip.Object) (has bool) {
	return false
}

// GetMethod returns the method if it exists.
func (obj *AckFuture) GetMethod(name string) *slip.Method {
	return nil
}

// MethodNames returns a sorted list of the methods of the class.
func (obj *AckFuture) MethodNames() slip.List {
	return slip.List{
		slip.Symbol(":change-class"),
		slip.Symbol(":change-flavor"),
		slip.Symbol(":describe"),
		slip.Symbol(":equal"),
		slip.Symbol(":eval-inside-yourself"),
		slip.Symbol(":flavor"),
		slip.Symbol(":id"),
		slip.Symbol(":inspect"),
		slip.Symbol(":message"),
		slip.Symbol(":operation-handled-p"),
		slip.Symbol(":print-self"),
		slip.Symbol(":result"),
		slip.Symbol(":send-if-handles"),
		slip.Symbol(":shared-initialize"),
		slip.Symbol(":update-instance-for-different-class"),
		slip.Symbol(":which-operations"),
	}
}

// Receive a method invocation from the send function. It is typically
// called by the send function but can be called directly so effectively
// send a method to an instance.
func (obj *AckFuture) Receive(s *slip.Scope, message string, args slip.List, depth int) (result slip.Object) {
	message = strings.ToLower(message)
	switch message {
	case ":result":
		result = obj.Pop()
	case ":message":
		if obj.Ack != nil {
			if m := obj.Ack.Msg(); m != nil {
				result = MakeMsg(&PubMsg{
					Body: m.Data,
					Head: m.Header,
					Subj: m.Subject,
					Repl: m.Reply,
				})
			}
		}
	case ":describe":
		obj.Describe(s, args, depth)
	case ":flavor", ":class":
		result = ackFutureFlavor
	case ":id":
		result = slip.Fixnum(obj.ID())
	case ":operation-handled-p":
		name, _ := args[0].(slip.Symbol)
		if obj.HasMethod(string(name)) {
			result = slip.True
		}
	case ":print-self":
		result = obj.PrintSelf(s, args, depth)
	case ":which-operations":
		result = obj.MethodNames()
	case ":change-class", ":change-flavor", ":shared-initialize", ":update-instance-for-different-class":
		slip.ErrorPanic(s, depth, "%s not allowed for %s", message, obj)
	case ":equal":
		if 0 < len(args) && obj == args[0] {
			result = slip.True
		}
	case ":eval-inside-yourself":
		if len(args) != 1 {
			slip.MethodArgCountPanic(s, depth, obj, ":eval-inside-yourself", len(args), 1, 1)
		}
		result = s.Eval(args[0], depth+1)
	case ":inspect":
		cf := slip.FindClass("bag-flavor")
		inst := cf.MakeInstance().(*flavors.Instance)
		inst.Any = map[string]any{
			"flavor": "jet-ack-future",
			"id":     int64(obj.ID()),
		}
		result = inst
	case ":send-if-handles":
		if len(args) == 0 {
			slip.MethodArgCountPanic(s, depth, obj, ":send-if-handles", len(args), 1, -1)
		}
		if sym, ok := args[0].(slip.Symbol); ok {
			if obj.HasMethod(string(sym)) {
				result = obj.Receive(s, string(sym), args[1:], depth+1)
			}
		}
	default:
		slip.NoApplicableMethodPanic(s, depth, obj, args, "")
	}
	return
}

// HasMethod returns true if the instance handles the named method.
func (obj *AckFuture) HasMethod(method string) bool {
	switch strings.ToLower(method) {
	case ":result", ":message":
		return true
	case ":describe", ":flavor", ":id", ":operation-handled-p", ":print-self", ":which-operations":
		return true
	}
	return false
}

// PrintSelf prints the instance.
func (obj *AckFuture) PrintSelf(s *slip.Scope, args slip.List, depth int) slip.Object {
	so := s.Get("*standard-output*")
	ss, _ := so.(slip.Stream)
	w := so.(io.Writer)
	if 0 < len(args) {
		var ok bool
		ss, _ = args[0].(slip.Stream)
		if w, ok = args[0].(io.Writer); !ok {
			slip.TypePanic(s, depth, ":describe output-stream", args[0], "output-stream")
		}
	}
	if _, err := w.Write(obj.Append(nil)); err != nil {
		slip.StreamPanic(s, depth, ss, "%s", err)
	}
	return nil
}

// Describe the object.
func (obj *AckFuture) Describe(s *slip.Scope, args slip.List, depth int) slip.Object {
	// Args should be stream print-depth escape-p. The second two arguments are
	// ignored.
	ansi := s.Get("*print-ansi*") != nil
	var b []byte
	if ansi {
		b = append(b, bold...)
		b = obj.Append(b)
		b = append(b, colorOff...)
	} else {
		b = obj.Append(b)
	}
	b = append(b, ", an instance of flavor "...)
	if ansi {
		b = append(b, bold...)
		b = append(b, "jet-ack-future"...)
		b = append(b, colorOff...)
	} else {
		b = append(b, "jet-ack-future"...)
	}
	b = append(b, ",\n"...)

	w := s.Get("*standard-output*").(io.Writer)
	if 0 < len(args) {
		if 1 < len(args) {
			slip.MethodArgCountPanic(s, depth, obj, ":describe", len(args), 0, 1)
		}
		var ok bool
		if w, ok = args[0].(io.Writer); !ok {
			slip.TypePanic(s, depth, ":describe output-stream", args[0], "output-stream")
		}
	}
	_, _ = w.Write(b)

	return nil
}

// MakeAckFuture makes a new jet-ack-future.
func MakeAckFuture(af jetstream.PubAckFuture) *AckFuture {
	return &AckFuture{
		Ack: af,
	}
}
