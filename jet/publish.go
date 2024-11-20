// Copyright (c) 2024, Peter Ohler, All rights reserved.

package jet

import (
	"github.com/ohler55/slip"
	"github.com/ohler55/slip/pkg/flavors"
)

type clientPublishCaller struct{}

func (caller clientPublishCaller) Call(s *slip.Scope, args slip.List, _ int) slip.Object {
	self := s.Get("self").(*flavors.Instance)
	flavors.CheckMethodArgCount(self, ":publish", len(args), 1, 20)

	// TBD return ack instance

	return nil
}

func (caller clientPublishCaller) Docs() string {
	return `__:publish__ _payload_ &optional _subject_ &key
_timeout_
_expect-last-msg-id_
_expect-last-sequence_
_expect-last-subject-sequence_
_expect-stream_
_msg-id_
_retry-attempts_
_retry-wait_
_stall-wait_
=> _jet-ack__
   _payload_ [octets|string|jet-msg] to publish as the content of a message.
   _subject_ [string] to publish the message on. It must be bound to a stream.
   _:timeout_ [real] for the publish.
   _:expect-last-msg-id_ [fixnum] sets the expected message ID the last message on a stream
should have. If the last message has a different message ID server will reject the message
and publish will fail.
   _:expect-last-sequence_ [fixnum] sets the expected sequence number the last message on a
stream should have. If the last message has a different sequence number server will reject
the message and publish will fail.
   _:expect-last-subject-sequence_ [fixnum] sets the expected sequence number the last message
on a subject the message is published to. If the last message on a subject has a different
sequence number server will reject the message and publish will fail.
   _:expect-stream_ [string] sets the expected stream the message should be published to. If
the message is published to a different stream server will reject the message and publish will
fail.
   _:msg-id_ [fixnum] sets the message ID used for deduplication.
   _:retry-attempts_ [fixnum] sets the retry number of attempts when ErrNoResponders is
encountered. Defaults to 2.
   _:retry-wait_ [real] sets the retry wait time in seconds when ErrNoResponders is encountered.
Defaults to 0.250 seconds.
   _:stall-wait_ [real] sets the max wait time in seconds when the producer becomes stall
producing messages. If a publish call is blocked for this long, ErrTooManyStalledMsgs is returned.


Performs a synchronous publish to a stream and waits for an ack from
server. It accepts a message payload and optional subject name (which must be
bound to a stream) which can be _octets_, _string_, or a _jet-msg_
instance. If the _payload_ is not a _jet-msg_ instance then the subject must
be provided. Multiple options in the form of keywords and values are
supported. An instace of the _jet-ack_ flavor is returned with information
about the published message.
`
}
