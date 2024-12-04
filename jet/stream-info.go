// Copyright (c) 2024, Peter Ohler, All rights reserved.

package jet

import (
	"context"

	"github.com/ohler55/slip"
	"github.com/ohler55/slip/pkg/flavors"

	_ "github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	_ "github.com/nats-io/nats.go/jetstream"
)

type streamInfoCaller struct{}

func (caller streamInfoCaller) Call(s *slip.Scope, args slip.List, _ int) slip.Object {
	self := s.Get("self").(*flavors.Instance)
	flavors.CheckMethodArgCount(self, ":info", len(args), 0, 4)
	stream := self.Any.(jetstream.Stream)
	var si *jetstream.StreamInfo

	if v, has := slip.GetArgsKeyValue(args, slip.Symbol(":cached")); has && v != nil {
		si = stream.CachedInfo()
	} else {
		ctx := context.Background()
		if v, has := slip.GetArgsKeyValue(args, slip.Symbol(":timeout")); has {
			var cf context.CancelFunc
			ctx, cf = context.WithTimeout(ctx, mustBeDuration(v, ":timeout"))
			defer cf()
		}
		var err error
		if si, err = stream.Info(ctx); err != nil {
			panic(err)
		}
	}
	return slip.Values{StreamStatePropList(&si.State), StreamConfigPropList(&si.Config)}
}

func (caller streamInfoCaller) Docs() string {
	return `__:info__ &key _timeout_ _cached_ => _state_[property list], _config_[property list]
   _:timeout_ [real] the number of seconds to wait before timing out.
   _:cached_ [boolean] return the cached information instead of fetching from the server.


Returns the stream information as a two part value of the stream state as a
property list and the stream configuration as a property list.
`
}

// StreamStatePropList return the stream state as a property list
func StreamStatePropList(ss *jetstream.StreamState) slip.List {
	deleted := make(slip.List, len(ss.Deleted))
	for i, d := range ss.Deleted {
		deleted[i] = slip.Fixnum(d)
	}
	subjects := make(slip.List, 0, 2*len(ss.Subjects))
	for subj, count := range ss.Subjects {
		subjects = append(subjects, slip.String(subj), slip.Fixnum(count))
	}
	return slip.List{
		slip.Symbol(":msgs"), slip.Fixnum(ss.Msgs),
		slip.Symbol(":bytes"), slip.Fixnum(ss.Bytes),
		slip.Symbol(":first-seq"), slip.Fixnum(ss.FirstSeq),
		slip.Symbol(":first-time"), slip.Time(ss.FirstTime),
		slip.Symbol(":last-seq"), slip.Fixnum(ss.LastSeq),
		slip.Symbol(":last-time"), slip.Time(ss.LastTime),
		slip.Symbol(":consumers"), slip.Fixnum(ss.Consumers),
		slip.Symbol(":deleted"), deleted,
		slip.Symbol(":number-deleted"), slip.Fixnum(ss.NumDeleted),
		slip.Symbol(":number-subjects"), slip.Fixnum(ss.NumSubjects),
		slip.Symbol(":subjects"), subjects,
	}
}
