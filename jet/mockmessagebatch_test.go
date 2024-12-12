// Copyright (c) 2024, Peter Ohler, All rights reserved.

package jet_test

import (
	"github.com/nats-io/nats.go/jetstream"
)

type mockMessageBatch struct {
	msgs []jetstream.Msg
	err  error
}

func (mb *mockMessageBatch) Messages() <-chan jetstream.Msg {
	msgChan := make(chan jetstream.Msg, len(mb.msgs))
	for _, m := range mb.msgs {
		msgChan <- m
	}
	close(msgChan)

	return msgChan
}

func (mb *mockMessageBatch) Error() error {
	return mb.err
}
