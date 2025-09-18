// Copyright (c) 2024, Peter Ohler, All rights reserved.

package jet

import (
	"github.com/ohler55/slip"
	"github.com/ohler55/slip/pkg/flavors"

	"github.com/nats-io/nats.go/jetstream"
)

var (
	consumeContextFlavor *flavors.Flavor
)

func defConsumeContext() {
	consumeContextFlavor = flavors.DefFlavor("jet-consume-context",
		map[string]slip.Object{},
		[]string{},
		slip.List{
			slip.List{
				slip.Symbol(":documentation"),
				slip.String(`An instance with information about a consume.`),
			},
		},
		&Pkg,
	)
	consumeContextFlavor.Final = true
	consumeContextFlavor.GoMakeOnly = true

	consumeContextFlavor.DefMethod(":stop", "", consumeContextStopCaller{})
	flavors.FlosFun("jet-consume-context-stop", ":stop", consumeContextStopCaller{}.FuncDocs(), &Pkg)

	consumeContextFlavor.DefMethod(":drain", "", consumeContextDrainCaller{})
	flavors.FlosFun("jet-consume-context-drain", ":drain", consumeContextDrainCaller{}.FuncDocs(), &Pkg)

	consumeContextFlavor.DefMethod(":closed", "", consumeContextClosedCaller{})
	flavors.FlosFun("jet-consume-context-closed", ":closed", consumeContextClosedCaller{}.FuncDocs(), &Pkg)
}

// MakeConsumeContext makes a jet-consume-context.
func MakeConsumeContext(mc jetstream.ConsumeContext) (inst *flavors.Instance) {
	inst = consumeContextFlavor.MakeInstance().(*flavors.Instance)
	inst.Any = mc

	return
}

type consumeContextStopCaller struct{}

func (caller consumeContextStopCaller) Call(s *slip.Scope, args slip.List, _ int) slip.Object {
	self := s.Get("self").(*flavors.Instance)
	cc := self.Any.(jetstream.ConsumeContext)

	cc.Stop()

	return nil
}

func (caller consumeContextStopCaller) FuncDocs() *slip.FuncDoc {
	return &slip.FuncDoc{
		Name: ":stop",
		Text: `Unsubscribes from the stream and cancels subscription.
No more messages will be received after calling this method.
All messages that are already in the buffer are discarded.`,
	}
}

type consumeContextDrainCaller struct{}

func (caller consumeContextDrainCaller) Call(s *slip.Scope, args slip.List, _ int) slip.Object {
	self := s.Get("self").(*flavors.Instance)
	cc := self.Any.(jetstream.ConsumeContext)

	cc.Drain()

	return nil
}

func (caller consumeContextDrainCaller) FuncDocs() *slip.FuncDoc {
	return &slip.FuncDoc{
		Name: ":drain",
		Text: `Unsubscribes from the stream and cancels subscription.
All messages that are already in the buffer will be processed in callback function.`,
	}
}

type consumeContextClosedCaller struct{}

func (caller consumeContextClosedCaller) Call(s *slip.Scope, args slip.List, _ int) slip.Object {
	self := s.Get("self").(*flavors.Instance)
	cc := self.Any.(jetstream.ConsumeContext)

	return TChannel(cc.Closed())
}

func (caller consumeContextClosedCaller) FuncDocs() *slip.FuncDoc {
	return &slip.FuncDoc{
		Name: ":closed",
		Text: `Returns a channel that is closed when the consuming is
fully stopped/drained. When the channel is closed, no more messages
will be received and processing is complete.`,
		Return: "channel",
	}
}
