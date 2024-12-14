// Copyright (c) 2024, Peter Ohler, All rights reserved.

package jet

import (
	"github.com/ohler55/slip"
	"github.com/ohler55/slip/pkg/flavors"

	"github.com/nats-io/nats.go/jetstream"
)

type consumerMessagesCaller struct{}

func (caller consumerMessagesCaller) Call(s *slip.Scope, args slip.List, _ int) slip.Object {
	self := s.Get("self").(*flavors.Instance)
	flavors.CheckMethodArgCount(self, ":messages", len(args), 0, 2)
	consumer := self.Any.(jetstream.Consumer)
	var opts []jetstream.PullMessagesOpt

	if v, has := slip.GetArgsKeyValue(args, slip.Symbol(":error-on-missing-heartbeat")); has {
		opts = append(opts, jetstream.WithMessagesErrOnMissingHeartbeat(v != nil))
	}
	mc, err := consumer.Messages(opts...)
	if err != nil {
		panic(err)
	}
	return MakeMessagesContext(mc)
}

func (caller consumerMessagesCaller) Docs() string {
	return `__:messages__ &key error-on-missing-heartbeat => _jet-message-context_
   _:error-on-missing-heartbeat_ [boolean] sets whether a missing heartbeat error should be
raised when calling [jet-messages-context :next] (Default: true).


Returns _jet-messages-context_, allowing continuously iterating
over messages on a stream.


Messages can be optimized for throughput or memory usage using
_:pull-expiry_, _:pull-max-messages_, _:pull-max-bytes_ and _:pull-heartbeat_
consumer configuration options. Unless there is a specific use case, these
options should not be used.


_:error-on-missing-heartbeat_ can be used to enable/disable
erroring out on the _jet-messages-context_ _:next_ method when a
heartbeat is missing. This option is enabled by default.
`
}
