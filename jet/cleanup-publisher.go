// Copyright (c) 2024, Peter Ohler, All rights reserved.

package jet

import (
	"github.com/ohler55/slip"
	"github.com/ohler55/slip/pkg/flavors"
)

type cleanupPublisherCaller struct{}

func (caller cleanupPublisherCaller) Call(s *slip.Scope, args slip.List, _ int) slip.Object {
	self := s.Get("self").(*flavors.Instance)
	slip.CheckMethodArgCount(self, ":cleanup-publisher", len(args), 0, 0)
	cl := self.Any.(*Client)
	cl.js.CleanupPublisher()

	return nil
}

func (caller cleanupPublisherCaller) Docs() string {
	return `__:cleanup-publisher__


Will cleanup the publishing side of a jet-client.


This will unsubscribe from the internal reply subject if needed. All pending
async publishes will fail with ErrJetStreamContextClosed.


If an error handler was provided, it will be called for each pending async
publish and PublishAsyncComplete will be closed.


After completing JetStreamContext is still usable - internal subscription will
be recreated on next publish, but the acks from previous publishes will be
lost.
`
}
