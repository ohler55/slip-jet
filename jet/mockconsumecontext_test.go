// Copyright (c) 2024, Peter Ohler, All rights reserved.

package jet_test

import "github.com/nats-io/nats.go/jetstream"

type mockConsumeContext struct {
	log     []byte
	msgs    []jetstream.Msg
	handler func(msg jetstream.Msg)
	closed  chan struct{}
}

func (cc *mockConsumeContext) Closed() <-chan struct{} {
	return cc.closed
}

func (cc *mockConsumeContext) Stop() {
	cc.log = append(cc.log, "Stop()\n"...)
}

func (cc *mockConsumeContext) Drain() {
	cc.log = append(cc.log, "Drain()\n"...)
	for _, m := range cc.msgs {
		cc.handler(m)
	}
}
