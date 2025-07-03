// Copyright (c) 2024, Peter Ohler, All rights reserved.

package jet

import (
	"github.com/ohler55/slip"
	"github.com/ohler55/slip/pkg/flavors"

	"github.com/nats-io/nats.go/jetstream"
)

var (
	messageBatchFlavor *flavors.Flavor
)

func defMessagesBatch() {
	messageBatchFlavor = flavors.DefFlavor("jet-message-batch",
		map[string]slip.Object{},
		[]string{},
		slip.List{
			slip.List{
				slip.Symbol(":documentation"),
				slip.String(`An instance that encompases a batch of messages.`),
			},
		},
		&Pkg,
	)
	messageBatchFlavor.Final = true
	messageBatchFlavor.GoMakeOnly = true

	messageBatchFlavor.DefMethod(":messages", "", messageBatchMessagesCaller{})
	flavors.FlosFun("jet-messages-batch-messages", ":messages", messageBatchMessagesCaller{}.FuncDocs(), &Pkg)

	messageBatchFlavor.DefMethod(":error", "", messageBatchErrorCaller{})
	flavors.FlosFun("jet-messages-batch-error", ":error", messageBatchErrorCaller{}.FuncDocs(), &Pkg)
}

// MakeMessagesBatch makes a jet-messages-batch.
func MakeMessageBatch(mb jetstream.MessageBatch) (inst *flavors.Instance) {
	inst = messageBatchFlavor.MakeInstance().(*flavors.Instance)
	inst.Any = mb

	return
}

type messageBatchMessagesCaller struct{}

func (caller messageBatchMessagesCaller) Call(s *slip.Scope, args slip.List, _ int) slip.Object {
	self := s.Get("self").(*flavors.Instance)
	mb := self.Any.(jetstream.MessageBatch)

	return MsgChannel(mb.Messages())
}

func (caller messageBatchMessagesCaller) FuncDocs() *slip.FuncDoc {
	return &slip.FuncDoc{
		Name:   ":messages",
		Text:   `Returns a _channel_ that provides the messages in the batch.`,
		Return: "channel",
	}
}

type messageBatchErrorCaller struct{}

func (caller messageBatchErrorCaller) Call(s *slip.Scope, args slip.List, _ int) (result slip.Object) {
	self := s.Get("self").(*flavors.Instance)
	mc := self.Any.(jetstream.MessageBatch)

	if err := mc.Error(); err != nil {
		result = slip.NewError("%s", err)
	}
	return
}

func (caller messageBatchErrorCaller) FuncDocs() *slip.FuncDoc {
	return &slip.FuncDoc{
		Name:   ":error",
		Text:   `Returns an _error_ or _nil_.`,
		Return: "error|nil",
	}
}
