// Copyright (c) 2025, Peter Ohler, All rights reserved.

package jet_test

import (
	"context"
	"fmt"

	"github.com/nats-io/nats.go/jetstream"
)

type mockPushConsumer struct {
	log  []byte
	info jetstream.ConsumerInfo
	cc   mockConsumeContext
	err  error
}

// Info returns ConsumerInfo.
func (mc *mockPushConsumer) Info(ctx context.Context) (*jetstream.ConsumerInfo, error) {
	return &mc.info, mc.err
}

// Info returns ConsumerInfo.
func (mc *mockPushConsumer) CachedInfo() *jetstream.ConsumerInfo {
	return &mc.info
}

func (mc *mockPushConsumer) Consume(
	handler jetstream.MessageHandler, opts ...jetstream.PushConsumeOpt) (jetstream.ConsumeContext, error) {
	mc.log = fmt.Appendf(mc.log, "Consume()\n")
	mc.cc.handler = handler
	for _, m := range mc.cc.msgs {
		handler(m)
	}
	err := mc.err
	if 0 < len(opts) && mc.err != nil {
		if eh, ok := opts[0].(jetstream.ConsumeErrHandler); ok {
			err = nil
			eh(&mc.cc, mc.err)
		}
	}
	return &mc.cc, err
}
