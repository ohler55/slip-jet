// Copyright (c) 2024, Peter Ohler, All rights reserved.

package jet

import (
	"context"

	"github.com/ohler55/slip"
	"github.com/ohler55/slip/pkg/flavors"

	"github.com/nats-io/nats.go/jetstream"
)

// This file includes the jet-stream-info flavor definition as well as the
// jet-stream :info method. Method for the jet-stream-info flavor are in file
// prefixed with info-.

var (
	streamInfoFlavor *flavors.Flavor
)

func defStreamInfo() {
	streamInfoFlavor = flavors.DefFlavor("jet-stream-info",
		map[string]slip.Object{},
		[]string{},
		slip.List{
			slip.List{
				slip.Symbol(":documentation"),
				slip.String(`TBD`),
			},
		},
		&Pkg,
	)
	streamInfoFlavor.Final = true
	streamInfoFlavor.GoMakeOnly = true

	streamInfoFlavor.DefMethod(":state", "", infoStateCaller{})
	flavors.FlosFun("jet-stream-info-state", ":state", infoStateCaller{}.Docs(), &Pkg)

	streamInfoFlavor.DefMethod(":created", "", infoCreatedCaller{})
	flavors.FlosFun("jet-stream-info-created", ":created", infoCreatedCaller{}.Docs(), &Pkg)

	streamInfoFlavor.DefMethod(":cluster", "", infoClusterCaller{})
	flavors.FlosFun("jet-stream-info-cluster", ":cluster", infoClusterCaller{}.Docs(), &Pkg)

	// TBD
	// mirror (stream-source-info as property list or instance?)
	// source
	// timestamp
}

// MakeStream makes a jet-stream.
func MakeStreamInfo(info *jetstream.StreamInfo) (inst *flavors.Instance) {
	inst = streamInfoFlavor.MakeInstance().(*flavors.Instance)
	inst.Any = info

	return
}

////////////////

type streamInfoCaller struct{}

func (caller streamInfoCaller) Call(s *slip.Scope, args slip.List, _ int) slip.Object {
	self := s.Get("self").(*flavors.Instance)
	flavors.CheckMethodArgCount(self, ":info", len(args), 0, 6)
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
		var opts []jetstream.StreamInfoOpt
		if v, has := slip.GetArgsKeyValue(args, slip.Symbol(":deleted")); has {
			opts = append(opts, jetstream.WithDeletedDetails(v != nil))
		}
		var err error
		if si, err = stream.Info(ctx, opts...); err != nil {
			panic(err)
		}
	}
	// TBD return stream-info instance
	return slip.Values{StreamStatePropList(&si.State), StreamConfigPropList(&si.Config)}
}

func (caller streamInfoCaller) Docs() string {
	return `__:info__ &key _timeout_ _cached_ _deleted_ => _jet-stream-state_
   _:timeout_ [real] the number of seconds to wait before timing out.
   _:cached_ [boolean] return the cached information instead of fetching from the server.
   _:deleted_ [boolean] if true, include the information about messages deleted from a stream.


Returns the stream information as an instance of the _jet-stream-info_ flavor.
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
