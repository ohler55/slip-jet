// Copyright (c) 2024, Peter Ohler, All rights reserved.

package jet_test

import (
	"context"
	"fmt"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/ohler55/ojg/pretty"
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
	var req jetstream.StreamPurgeRequest
	for _, opt := range opts {
		_ = opt(&req)
	}
	if _, ok := ctx.Deadline(); ok {
		ms.log = fmt.Appendf(ms.log, "Purge called with %s with deadline\n", pretty.SEN(&req))
	} else {
		ms.log = fmt.Appendf(ms.log, "Purge called with %s\n", pretty.SEN(&req))
	}
	return ms.err
}

// GetMsg returns a sequence number.
func (ms *mockStream) GetMsg(
	ctx context.Context, seq uint64, opts ...jetstream.GetMsgOpt) (*jetstream.RawStreamMsg, error) {
	if _, ok := ctx.Deadline(); ok {
		ms.log = fmt.Appendf(ms.log, "GetMsg %d called with timeout\n", seq)
	} else {
		ms.log = fmt.Appendf(ms.log, "GetMsg %d called\n", seq)
	}
	return &jetstream.RawStreamMsg{
		Subject:  "test.sub",
		Sequence: 123,
		Header:   nats.Header{"head": []string{"tail"}},
		Data:     []byte("hello"),
		Time:     time.Date(2024, time.December, 5, 6, 7, 8, 9, time.UTC),
	}, ms.err
}

// GetLastMsgForSubject ...
func (ms *mockStream) GetLastMsgForSubject(ctx context.Context, subject string) (*jetstream.RawStreamMsg, error) {
	if _, ok := ctx.Deadline(); ok {
		ms.log = fmt.Appendf(ms.log, "GetLastMsgForSubject %s called with timeout\n", subject)
	} else {
		ms.log = fmt.Appendf(ms.log, "GetLastMsgForSubject %s called\n", subject)
	}
	return &jetstream.RawStreamMsg{
		Subject:  "test.sub",
		Sequence: 123,
		Header:   nats.Header{"head": []string{"tail"}},
		Data:     []byte("hello"),
		Time:     time.Date(2024, time.December, 5, 6, 7, 8, 9, time.UTC),
	}, ms.err
}

// DeleteMsg logs the call.
func (ms *mockStream) DeleteMsg(ctx context.Context, seq uint64) error {
	if _, ok := ctx.Deadline(); ok {
		ms.log = fmt.Appendf(ms.log, "DeleteMsg %d called with timeout\n", seq)
	} else {
		ms.log = fmt.Appendf(ms.log, "DeleteMsg %d called\n", seq)
	}
	return ms.err
}

// SecureDeleteMsg logs the call.
func (ms *mockStream) SecureDeleteMsg(ctx context.Context, seq uint64) error {
	if _, ok := ctx.Deadline(); ok {
		ms.log = fmt.Appendf(ms.log, "SecureDeleteMsg %d called with timeout\n", seq)
	} else {
		ms.log = fmt.Appendf(ms.log, "SecureDeleteMsg %d called\n", seq)
	}
	return ms.err
}

// CreateOrUpdateConsumer logs the call.
func (ms *mockStream) CreateOrUpdateConsumer(
	ctx context.Context, cfg jetstream.ConsumerConfig) (jetstream.Consumer, error) {
	return nil, ms.err
}

// CreateConsumer logs the call.
func (ms *mockStream) CreateConsumer(ctx context.Context, cfg jetstream.ConsumerConfig) (jetstream.Consumer, error) {
	return nil, ms.err
}

// UpdateConsumer logs the call.
func (ms *mockStream) UpdateConsumer(ctx context.Context, cfg jetstream.ConsumerConfig) (jetstream.Consumer, error) {
	return nil, ms.err
}

// OrderedConsumer logs the call.
func (ms *mockStream) OrderedConsumer(
	ctx context.Context, cfg jetstream.OrderedConsumerConfig) (jetstream.Consumer, error) {
	return nil, ms.err
}

// Consumer logs the call.
func (ms *mockStream) Consumer(ctx context.Context, consumer string) (jetstream.Consumer, error) {
	return nil, ms.err
}

// DeleteConsumer logs the call.
func (ms *mockStream) DeleteConsumer(ctx context.Context, consumer string) error {
	return ms.err
}

// ListConsumers logs the call.
func (ms *mockStream) ListConsumers(context.Context) jetstream.ConsumerInfoLister {
	return nil
}

// ConsumerNames logs the call.
func (ms *mockStream) ConsumerNames(context.Context) jetstream.ConsumerNameLister {
	return nil
}

// CreatePushConsumer does nothing.
func (ms *mockStream) CreatePushConsumer(
	ctx context.Context, cfg jetstream.ConsumerConfig) (jetstream.PushConsumer, error) {
	return nil, ms.err
}

// CreateOrUpdatePushConsumer does nothing.
func (ms *mockStream) CreateOrUpdatePushConsumer(
	ctx context.Context, cfg jetstream.ConsumerConfig) (jetstream.PushConsumer, error) {
	return nil, ms.err
}

// PauseConsumer does nothing
func (ms *mockStream) PauseConsumer(
	ctx context.Context, consumer string, pauseUntil time.Time) (*jetstream.ConsumerPauseResponse, error) {
	return nil, ms.err
}

// ResumeConsumer does nothing
func (ms *mockStream) ResumeConsumer(
	ctx context.Context, consumer string) (*jetstream.ConsumerPauseResponse, error) {
	return nil, ms.err
}

// PushConsumer does nothing.
func (ms *mockStream) PushConsumer(ctx context.Context, consumer string) (jetstream.PushConsumer, error) {
	return nil, ms.err
}

// UnpinConsumer does nothing.
func (ms *mockStream) UnpinConsumer(ctx context.Context, consumer string, group string) error {
	return ms.err
}

// UpdatePushConsumer does nothing.
func (ms *mockStream) UpdatePushConsumer(
	ctx context.Context, cfg jetstream.ConsumerConfig) (jetstream.PushConsumer, error) {
	return nil, ms.err
}

// ResetConsumer does nothing.
func (ms *mockStream) ResetConsumer(ctx context.Context, consumer string) (*jetstream.ConsumerResetResponse, error) {
	return nil, ms.err
}

// ResetConsumerToSequence does nothing
func (ms *mockStream) ResetConsumerToSequence(
	ctx context.Context, consumer string, seq uint64) (*jetstream.ConsumerResetResponse, error) {
	return nil, ms.err
}
