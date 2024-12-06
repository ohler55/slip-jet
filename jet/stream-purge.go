// Copyright (c) 2024, Peter Ohler, All rights reserved.

package jet

import (
	"context"

	"github.com/ohler55/slip"
	"github.com/ohler55/slip/pkg/flavors"

	"github.com/nats-io/nats.go/jetstream"
)

type streamPurgeCaller struct{}

func (caller streamPurgeCaller) Call(s *slip.Scope, args slip.List, _ int) slip.Object {
	self := s.Get("self").(*flavors.Instance)
	flavors.CheckMethodArgCount(self, ":purge", len(args), 0, 8)
	stream := self.Any.(jetstream.Stream)

	ctx := context.Background()
	if v, has := slip.GetArgsKeyValue(args, slip.Symbol(":timeout")); has {
		var cf context.CancelFunc
		ctx, cf = context.WithTimeout(ctx, mustBeDuration(v, ":timeout"))
		defer cf()
	}
	var opts []jetstream.StreamPurgeOpt

	if v, has := slip.GetArgsKeyValue(args, slip.Symbol(":keep")); has {
		if num, ok := v.(slip.Fixnum); ok {
			opts = append(opts, jetstream.WithPurgeKeep(uint64(num)))
		} else {
			slip.PanicType(":keep", v, "fixnum")
		}
	}
	if v, has := slip.GetArgsKeyValue(args, slip.Symbol(":sequence")); has {
		if num, ok := v.(slip.Fixnum); ok {
			opts = append(opts, jetstream.WithPurgeSequence(uint64(num)))
		} else {
			slip.PanicType(":sequence", v, "fixnum")
		}
	}
	if v, has := slip.GetArgsKeyValue(args, slip.Symbol(":subject")); has {
		opts = append(opts, jetstream.WithPurgeSubject(slip.MustBeString(v, ":subject")))
	}
	if err := stream.Purge(ctx, opts...); err != nil {
		panic(err)
	}
	return nil
}

func (caller streamPurgeCaller) Docs() string {
	return `__:purge__ &key _timeout_ _keep_ _sequence_ _subject_
   _:timeout_ [real] the number of seconds to wait before timing out.
   _:keep_ [fixnum] sets the number of messages to be kept in the stream after purge.
Can be combined with the _:subject_ option, but not with _:sequence_ option.
   _:sequence_ [fixnum] used to set a specific sequence number up to which
(but not including) messages will be purged from a stream. Can be combined with
the _:subject_ option, but not with the _:keep_ option.
   _:subject_ [string] sets a specific subject for which messages on a stream will be purged.


Removes messages from a stream. It is a destructive operation. Use with caution.
`
}
