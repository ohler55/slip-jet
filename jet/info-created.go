// Copyright (c) 2024, Peter Ohler, All rights reserved.

package jet

import (
	"github.com/nats-io/nats.go/jetstream"
	"github.com/ohler55/slip"
	"github.com/ohler55/slip/pkg/flavors"
)

type infoCreatedCaller struct{}

func (caller infoCreatedCaller) Call(s *slip.Scope, args slip.List, _ int) slip.Object {
	self := s.Get("self").(*flavors.Instance)
	si := self.Any.(jetstream.StreamInfo)

	return slip.Time(si.Created)
}

func (caller infoCreatedCaller) Docs() string {
	return `__:created__ => _time_


Returns the timestamp when the stream was created.
`
}
