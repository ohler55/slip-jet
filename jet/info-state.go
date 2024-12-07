// Copyright (c) 2024, Peter Ohler, All rights reserved.

package jet

import (
	"github.com/nats-io/nats.go/jetstream"
	"github.com/ohler55/slip"
	"github.com/ohler55/slip/pkg/flavors"
)

type infoStateCaller struct{}

func (caller infoStateCaller) Call(s *slip.Scope, args slip.List, _ int) slip.Object {
	self := s.Get("self").(*flavors.Instance)
	si := self.Any.(jetstream.StreamInfo)

	return MakeStreamState(&si.State)
}

func (caller infoStateCaller) Docs() string {
	return `__:state__ => _jet-stream-state_


Returns the stream information as an instance of the _jet-stream-info_ flavor.
`
}
