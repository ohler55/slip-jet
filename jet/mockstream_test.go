// Copyright (c) 2024, Peter Ohler, All rights reserved.

package jet_test

import (
	"context"

	"github.com/nats-io/nats.go/jetstream"
)

type mockStream struct {
	log  []byte
	info jetstream.StreamInfo
	err  error
}

// Info returns StreamInfo.
func (ms *mockStream) Info(ctx context.Context, opts ...jetstream.StreamInfoOpt) (*jetstream.StreamInfo, error) {
	return &ms.info, ms.err
}

// Info returns StreamInfo.
func (ms *mockStream) CachedInfo() *jetstream.StreamInfo {
	return &ms.info
}

// Purge logs the call.
func (ms *mockStream) Purge(ctx context.Context, opts ...jetstream.StreamPurgeOpt) error {
	ms.log = append(ms.log, "Purge called\n"...)
	return ms.err
}

// GetMsg returns a sequence number.
func (ms *mockStream) GetMsg(
	ctx context.Context, seq uint64, opts ...jetstream.GetMsgOpt) (*jetstream.RawStreamMsg, error) {
	// TBD
	return nil, ms.err
}

// GetLastMsgForSubject ...
func (ms *mockStream) GetLastMsgForSubject(ctx context.Context, subject string) (*jetstream.RawStreamMsg, error) {
	// TBD
	return nil, ms.err
}

// DeleteMsg logs the call.
func (ms *mockStream) DeleteMsg(ctx context.Context, seq uint64) error {
	return ms.err
}

// SecureDeleteMsg logs the call.
func (ms *mockStream) SecureDeleteMsg(ctx context.Context, seq uint64) error {
	// TBD
	return ms.err
}

// CreateOrUpdateConsumer logs the call.
func (ms *mockStream) CreateOrUpdateConsumer(
	ctx context.Context, cfg jetstream.ConsumerConfig) (jetstream.Consumer, error) {
	// TBD
	return nil, ms.err
}

// CreateConsumer logs the call.
func (ms *mockStream) CreateConsumer(ctx context.Context, cfg jetstream.ConsumerConfig) (jetstream.Consumer, error) {
	// TBD
	return nil, ms.err
}

// UpdateConsumer logs the call.
func (ms *mockStream) UpdateConsumer(ctx context.Context, cfg jetstream.ConsumerConfig) (jetstream.Consumer, error) {
	// TBD
	return nil, ms.err
}

// OrderedConsumer logs the call.
func (ms *mockStream) OrderedConsumer(
	ctx context.Context, cfg jetstream.OrderedConsumerConfig) (jetstream.Consumer, error) {
	// TBD
	return nil, ms.err
}

// Consumer logs the call.
func (ms *mockStream) Consumer(ctx context.Context, consumer string) (jetstream.Consumer, error) {
	// TBD
	return nil, ms.err
}

// DeleteConsumer logs the call.
func (ms *mockStream) DeleteConsumer(ctx context.Context, consumer string) error {
	// TBD
	return ms.err
}

// ListConsumers logs the call.
func (ms *mockStream) ListConsumers(context.Context) jetstream.ConsumerInfoLister {
	// TBD
	return nil
}

// ConsumerNames logs the call.
func (ms *mockStream) ConsumerNames(context.Context) jetstream.ConsumerNameLister {
	// TBD
	return nil
}
