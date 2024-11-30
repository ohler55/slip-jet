// Copyright (c) 2024, Peter Ohler, All rights reserved.

package jet_test

import (
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

type mockAckFuture struct {
	okChan  chan *jetstream.PubAck
	errChan chan error
	msg     *nats.Msg
}

// Ok returns a receive only channel that can be used to get a PubAck.
func (af *mockAckFuture) Ok() <-chan *jetstream.PubAck {
	return af.okChan
}

// Err returns a receive only channel that can be used to get the error from an async publish.
func (af *mockAckFuture) Err() <-chan error {
	return af.errChan
}

// Msg returns the message that was sent to the server.
func (af *mockAckFuture) Msg() *nats.Msg {
	return af.msg
}
