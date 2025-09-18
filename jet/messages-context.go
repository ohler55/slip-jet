// Copyright (c) 2024, Peter Ohler, All rights reserved.

package jet

import (
	"github.com/ohler55/slip"
	"github.com/ohler55/slip/pkg/flavors"

	"github.com/nats-io/nats.go/jetstream"
)

var (
	messagesContextFlavor *flavors.Flavor
)

func defMessagesContext() {
	messagesContextFlavor = flavors.DefFlavor("jet-messages-context",
		map[string]slip.Object{},
		[]string{},
		slip.List{
			slip.List{
				slip.Symbol(":documentation"),
				slip.String(`An instance with information about a messages.`),
			},
		},
		&Pkg,
	)
	messagesContextFlavor.Final = true
	messagesContextFlavor.GoMakeOnly = true

	messagesContextFlavor.DefMethod(":next", "", messagesContextNextCaller{})
	flavors.FlosFun("jet-messages-context-next", ":next", messagesContextNextCaller{}.FuncDocs(), &Pkg)

	messagesContextFlavor.DefMethod(":stop", "", messagesContextStopCaller{})
	flavors.FlosFun("jet-messages-context-stop", ":stop", messagesContextStopCaller{}.FuncDocs(), &Pkg)

	messagesContextFlavor.DefMethod(":drain", "", messagesContextDrainCaller{})
	flavors.FlosFun("jet-messages-context-drain", ":drain", messagesContextDrainCaller{}.FuncDocs(), &Pkg)

}

// MakeMessagesContext makes a jet-messages-context.
func MakeMessagesContext(mc jetstream.MessagesContext) (inst *flavors.Instance) {
	inst = messagesContextFlavor.MakeInstance().(*flavors.Instance)
	inst.Any = mc

	return
}

type messagesContextNextCaller struct{}

func (caller messagesContextNextCaller) Call(s *slip.Scope, args slip.List, _ int) slip.Object {
	self := s.Get("self").(*flavors.Instance)
	mc := self.Any.(jetstream.MessagesContext)

	m, err := mc.Next()
	if err != nil {
		panic(err)
	}
	return MakeMsg(m)
}

func (caller messagesContextNextCaller) FuncDocs() *slip.FuncDoc {
	return &slip.FuncDoc{
		Name:   ":next",
		Text:   `Retrieves next message on a stream. It will block until the next message is available.`,
		Return: "jet-msg",
	}
}

type messagesContextStopCaller struct{}

func (caller messagesContextStopCaller) Call(s *slip.Scope, args slip.List, _ int) slip.Object {
	self := s.Get("self").(*flavors.Instance)
	mc := self.Any.(jetstream.MessagesContext)

	mc.Stop()

	return nil
}

func (caller messagesContextStopCaller) FuncDocs() *slip.FuncDoc {
	return &slip.FuncDoc{
		Name: ":stop",
		Text: `Unsubscribes from the stream and cancels subscription. Calling _:next_ after calling
_:stop_ will raise an error. All messages that are already in the buffer are discarded.`,
	}
}

type messagesContextDrainCaller struct{}

func (caller messagesContextDrainCaller) Call(s *slip.Scope, args slip.List, _ int) slip.Object {
	self := s.Get("self").(*flavors.Instance)
	mc := self.Any.(jetstream.MessagesContext)

	mc.Drain()

	return nil
}

func (caller messagesContextDrainCaller) FuncDocs() *slip.FuncDoc {
	return &slip.FuncDoc{
		Name: ":drain",
		Text: `Unsubscribes from the stream and cancels subscription. All messages that are already
in the buffer will be available on subsequent calls to _:next_. After the buffer
is drained, _:next_ will raise an error.`,
	}
}
