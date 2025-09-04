// Copyright (c) 2025, Peter Ohler, All rights reserved.

package jet

import (
	"context"

	"github.com/ohler55/slip"
	"github.com/ohler55/slip/pkg/cl"
	"github.com/ohler55/slip/pkg/flavors"

	"github.com/nats-io/nats.go/jetstream"
)

var (
	pushConsumerFlavor *flavors.Flavor
)

func defPushConsumer() {
	pushConsumerFlavor = flavors.DefFlavor("jet-push-consumer",
		map[string]slip.Object{},
		[]string{},
		slip.List{
			slip.List{
				slip.Symbol(":documentation"),
				slip.String(`Consumer contains methods for fetching/processing messages from
a stream, as well as fetching consumer info.
`),
			},
		},
		&Pkg,
	)
	pushConsumerFlavor.Final = true

	pushConsumerFlavor.DefMethod(":info", "", pushConsumerInfoCaller{})
	flavors.FlosFun("jet-push-consumer-info", ":info", pushConsumerInfoCaller{}.FuncDocs(), &Pkg)

	pushConsumerFlavor.DefMethod(":consume", "", pushConsumerConsumeCaller{})
	flavors.FlosFun("jet-push-consumer-consume", ":consume", pushConsumerConsumeCaller{}.FuncDocs(), &Pkg)
}

// MakePushConsumer makes a jet-stream.
func MakePushConsumer(consumer jetstream.PushConsumer) (inst *flavors.Instance) {
	inst = pushConsumerFlavor.MakeInstance().(*flavors.Instance)
	inst.Any = consumer

	return
}

type pushConsumerInfoCaller struct{}

func (caller pushConsumerInfoCaller) Call(s *slip.Scope, args slip.List, depth int) slip.Object {
	self := s.Get("self").(*flavors.Instance)
	slip.MethodArgCountCheck(s, depth, self, ":info", len(args), 0, 6)
	consumer := self.Any.(jetstream.PushConsumer)
	var ci *jetstream.ConsumerInfo

	if v, has := slip.GetArgsKeyValue(args, slip.Symbol(":cached")); has && v != nil {
		ci = consumer.CachedInfo()
	} else {
		ctx := context.Background()
		if v, has := slip.GetArgsKeyValue(args, slip.Symbol(":timeout")); has {
			var cf context.CancelFunc
			ctx, cf = context.WithTimeout(ctx, mustBeDuration(s, v, ":timeout", depth))
			defer cf()
		}
		var err error
		if ci, err = consumer.Info(ctx); err != nil {
			panic(err)
		}
	}
	return MakeConsumerInfo(ci)
}

func (caller pushConsumerInfoCaller) FuncDocs() *slip.FuncDoc {
	return &slip.FuncDoc{
		Name: ":info",
		Text: `Returns the consumer information as an instance of the _jet-consumer-info_ flavor.`,
		Args: []*slip.DocArg{
			{Name: "&key"},
			{
				Name: ":timeout",
				Type: "real",
				Text: "The number of seconds to wait before timing out.",
			},
			{
				Name: ":cached",
				Type: "boolean",
				Text: "Return the cached information instead of fetching from the server.",
			},
		},
		Return: "_jet-consumer-info",
	}
}

type pushConsumerConsumeCaller struct{}

func (caller pushConsumerConsumeCaller) Call(s *slip.Scope, args slip.List, depth int) slip.Object {
	self := s.Get("self").(*flavors.Instance)
	slip.MethodArgCountCheck(s, depth, self, ":consume", len(args), 1, 17)
	consumer := self.Any.(jetstream.PushConsumer)

	var opts []jetstream.PushConsumeOpt
	msgCaller := cl.ResolveToCaller(s, args[0], 0)

	args = args[1:]
	if v, has := slip.GetArgsKeyValue(args, slip.Symbol(":error-handler")); has {
		errCaller := cl.ResolveToCaller(s, v, 0)
		opts = append(opts, jetstream.ConsumeErrHandler(
			func(cc jetstream.ConsumeContext, err error) {
				_ = errCaller.Call(s, slip.List{MakeConsumeContext(cc), slip.ErrorNew(s, depth, "%s", err)}, 0)
			}))
	}
	cc, err := consumer.Consume(func(msg jetstream.Msg) {
		_ = msgCaller.Call(s, slip.List{MakeMsg(msg)}, 0)
	}, opts...)
	if err != nil {
		panic(err)
	}
	return MakeConsumeContext(cc)
}

func (caller pushConsumerConsumeCaller) FuncDocs() *slip.FuncDoc {
	return &slip.FuncDoc{
		Name: ":consume",
		Text: `Will continuously receive messages and handle them
with the provided callback function. _:consume_ can be configured
using then _:error-handler_ options.


Error handling and monitoring can be configured using _:error-handler_ option,
which provides information about errors encountered during consumption
(both transient and terminal)


Returns a _jet-consume-context_, which can be used to stop or drain the consumer.
`,
		Args: []*slip.DocArg{
			{
				Name: "message-handler",
				Type: "function",
				Text: "A function that expects one message argument.",
			},
			{Name: "&key"},
			{
				Name: ":error-handler",
				Type: "function",
				Text: `A function that is called on error with two arguments. The
first argument is a _jet-consumer-context_ and the second is the error that caused the
handler to be called.`,
			},
		},
		Return: "jet-consume-context",
	}
}
