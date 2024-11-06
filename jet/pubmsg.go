// Copyright (c) 2024, Peter Ohler, All rights reserved.

package jet

import (
	"context"
	"fmt"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// PubMsg bridges the disconnect between the nats.go package and the
// nats.go/jetstream package. A nats.Msg does not implement the jetstream.Msg
// interface so to avoid forcing the developer to be aware of the two similar
// but different Msg types this struct is used for messages that have not been
// received by a consumer or a message for publishing.
type PubMsg struct {
	Meta *jetstream.MsgMetadata
	Body []byte
	Head nats.Header
	Subj string
	Repl string
}

// Metadata returns [MsgMetadata] for a JetStream message.
func (pm *PubMsg) Metadata() (*jetstream.MsgMetadata, error) {
	if pm.Meta == nil {
		return nil, fmt.Errorf("can not get the metadata for an unpublished message")
	}
	return pm.Meta, nil
}

// Data returns the message body.
func (pm *PubMsg) Data() []byte {
	return pm.Body
}

// Headers returns a map of headers for a message.
func (pm *PubMsg) Headers() nats.Header {
	return pm.Head
}

// Subject returns a subject on which a message was published/received.
func (pm *PubMsg) Subject() string {
	return pm.Subj
}

// Reply returns a reply subject for a message.
func (pm *PubMsg) Reply() string {
	return pm.Repl
}

// Ack will always return an error since the message was not received and
// hence has nowhere to send the ack to.
func (pm *PubMsg) Ack() error {
	return fmt.Errorf("can not ACK an unpublished message")
}

// DoubleAck will always return an error since the message was not received
// and hence has nowhere to send the ack to.
func (pm *PubMsg) DoubleAck(_ context.Context) error {
	return fmt.Errorf("can not ACK an unpublished message")
}

// Nak will always return an error since the message was not received and
// hence has nowhere to send the NAK to.
func (pm *PubMsg) Nak() error {
	return fmt.Errorf("can not NAK an unpublished message")
}

// NakWithDelay will always return an error since the message was not received
// and hence has nowhere to send the NAK to.
func (pm *PubMsg) NakWithDelay(delay time.Duration) error {
	return fmt.Errorf("can not NAK an unpublished message")
}

// InProgress will always return an error since the message was not received
// so can not be in progress.
func (pm *PubMsg) InProgress() error {
	return fmt.Errorf("can not indicate an unpublished message is in-progress")
}

// Term will always return an error since the message was not received
// so can not be terminated.
func (pm *PubMsg) Term() error {
	return fmt.Errorf("can not terminate an unpublished message")
}

// Term will always return an error since the message was not received
// so can not be terminated.
func (pm *PubMsg) TermWithReason(reason string) error {
	return fmt.Errorf("can not terminate an unpublished message")
}
