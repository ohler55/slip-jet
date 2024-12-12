// Copyright (c) 2024, Peter Ohler, All rights reserved.

package jet_test

import (
	"context"
	"fmt"

	"github.com/nats-io/nats.go/jetstream"
)

type mockConsumer struct {
	log  []byte
	info jetstream.ConsumerInfo
	mb   jetstream.MessageBatch
	err  error
}

// Info returns ConsumerInfo.
func (mc *mockConsumer) Info(ctx context.Context) (*jetstream.ConsumerInfo, error) {
	return &mc.info, mc.err
}

// Info returns ConsumerInfo.
func (mc *mockConsumer) CachedInfo() *jetstream.ConsumerInfo {
	return &mc.info
}

func (mc *mockConsumer) Fetch(batch int, opts ...jetstream.FetchOpt) (jetstream.MessageBatch, error) {
	// TBD
	mc.log = fmt.Appendf(mc.log, "Fetch(%d)\n", batch)

	return mc.mb, mc.err
}

func (mc *mockConsumer) FetchBytes(maxBytes int, opts ...jetstream.FetchOpt) (jetstream.MessageBatch, error) {
	// TBD
	mc.log = fmt.Appendf(mc.log, "FetchBytes(%d)\n", maxBytes)

	return nil, mc.err
}

func (mc *mockConsumer) FetchNoWait(batch int) (jetstream.MessageBatch, error) {
	// TBD
	mc.log = fmt.Appendf(mc.log, "FetchNoWait(%d)\n", batch)

	return mc.mb, mc.err
}

func (mc *mockConsumer) Consume(
	handler jetstream.MessageHandler, opts ...jetstream.PullConsumeOpt) (jetstream.ConsumeContext, error) {
	// TBD
	mc.log = fmt.Appendf(mc.log, "Consume()\n")

	return nil, mc.err
}

func (mc *mockConsumer) Messages(opts ...jetstream.PullMessagesOpt) (jetstream.MessagesContext, error) {
	// TBD
	mc.log = fmt.Appendf(mc.log, "Messages()\n")

	return nil, mc.err
}

func (mc *mockConsumer) Next(opts ...jetstream.FetchOpt) (jetstream.Msg, error) {
	// TBD
	mc.log = fmt.Appendf(mc.log, "Next()\n")

	return nil, mc.err
}
