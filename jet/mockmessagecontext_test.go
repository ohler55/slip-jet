// Copyright (c) 2024, Peter Ohler, All rights reserved.

package jet_test

import (
	"github.com/nats-io/nats.go/jetstream"
)

type mockMessageContext struct {
	log []byte
	msg jetstream.Msg
	err error
}

func (mc *mockMessageContext) Next(opts ...jetstream.NextOpt) (jetstream.Msg, error) {
	return mc.msg, mc.err
}

func (mc *mockMessageContext) Stop() {
	mc.log = append(mc.log, "Stop()\n"...)
}

func (mc *mockMessageContext) Drain() {
	mc.log = append(mc.log, "Drain()\n"...)
}
