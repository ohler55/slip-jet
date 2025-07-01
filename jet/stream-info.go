// Copyright (c) 2024, Peter Ohler, All rights reserved.

package jet

import (
	"context"
	"time"

	"github.com/ohler55/slip"
	"github.com/ohler55/slip/pkg/flavors"

	"github.com/nats-io/nats.go/jetstream"
)

// This file includes the jet-stream-info flavor definition as well as the
// jet-stream :info method.

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
				slip.String(`An instance with information about a stream.`),
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

	streamInfoFlavor.DefMethod(":mirror", "", infoMirrorCaller{})
	flavors.FlosFun("jet-stream-info-mirror", ":mirror", infoMirrorCaller{}.Docs(), &Pkg)

	streamInfoFlavor.DefMethod(":sources", "", infoSourcesCaller{})
	flavors.FlosFun("jet-stream-info-sources", ":sources", infoSourcesCaller{}.Docs(), &Pkg)

	streamInfoFlavor.DefMethod(":timestamp", "", infoTimestampCaller{})
	flavors.FlosFun("jet-stream-info-timestamp", ":timestamp", infoTimestampCaller{}.Docs(), &Pkg)
}

// MakeStream makes a jet-stream-info.
func MakeStreamInfo(info *jetstream.StreamInfo) (inst *flavors.Instance) {
	inst = streamInfoFlavor.MakeInstance().(*flavors.Instance)
	inst.Any = info

	return
}

type infoCreatedCaller struct{}

func (caller infoCreatedCaller) Call(s *slip.Scope, args slip.List, _ int) slip.Object {
	self := s.Get("self").(*flavors.Instance)
	si := self.Any.(*jetstream.StreamInfo)

	return slip.Time(si.Created)
}

func (caller infoCreatedCaller) Docs() string {
	return `__:created__ => _time_


Returns the timestamp when the stream was created.
`
}

type infoStateCaller struct{}

func (caller infoStateCaller) Call(s *slip.Scope, args slip.List, _ int) slip.Object {
	self := s.Get("self").(*flavors.Instance)
	si := self.Any.(*jetstream.StreamInfo)

	return MakeStreamState(&si.State)
}

func (caller infoStateCaller) Docs() string {
	return `__:state__ => _jet-stream-state_


Returns the stream information as an instance of the _jet-stream-info_ flavor.
`
}

type infoClusterCaller struct{}

func (caller infoClusterCaller) Call(s *slip.Scope, args slip.List, _ int) (result slip.Object) {
	self := s.Get("self").(*flavors.Instance)
	si := self.Any.(*jetstream.StreamInfo)
	if si.Cluster != nil {
		cluster := si.Cluster
		var reps slip.List
		for _, pi := range cluster.Replicas {
			var (
				current slip.Object
				offline slip.Object
			)
			if pi.Current {
				current = slip.True
			}
			if pi.Offline {
				offline = slip.True
			}
			reps = append(reps, slip.List{
				slip.Symbol(":name"), slip.String(pi.Name),
				slip.Symbol(":current"), current,
				slip.Symbol(":offline"), offline,
				slip.Symbol(":active"), slip.DoubleFloat(float64(pi.Active) / float64(time.Second)),
				slip.Symbol(":lag"), slip.Fixnum(pi.Lag),
			})
		}
		result = slip.List{
			slip.Symbol(":name"), slip.String(cluster.Name),
			slip.Symbol(":leader"), slip.String(cluster.Leader),
			slip.Symbol(":replicas"), reps,
		}
	}
	return
}

func (caller infoClusterCaller) Docs() string {
	return `__:cluster__ => _property list_


Returns the information about the cluster to which this stream belongs (if
applicable). The properties are _:name_, _:leader_, and _:replicas_. The
_:replicas_ property is a property list of _:name_, _:current_, _:offline_,
_:active_, and _:lag_.
`
}

type infoMirrorCaller struct{}

func (caller infoMirrorCaller) Call(s *slip.Scope, args slip.List, _ int) (result slip.Object) {
	self := s.Get("self").(*flavors.Instance)
	si := self.Any.(*jetstream.StreamInfo)
	if si.Mirror != nil {
		result = MakeStreamSourceInfo(si.Mirror)
	}
	return
}

func (caller infoMirrorCaller) Docs() string {
	return `__:mirror__ => _jet-stream-source-info_


Returns information about another stream this one is mirroring. Mirroring
is used to create replicas of another stream's  data. This field is omitted
if the stream is not mirroring another stream.
`
}

type infoSourcesCaller struct{}

func (caller infoSourcesCaller) Call(s *slip.Scope, args slip.List, _ int) slip.Object {
	self := s.Get("self").(*flavors.Instance)
	si := self.Any.(*jetstream.StreamInfo)
	var srcs slip.List
	for _, src := range si.Sources {
		srcs = append(srcs, MakeStreamSourceInfo(src))
	}
	return srcs
}

func (caller infoSourcesCaller) Docs() string {
	return `__:sources__ => _list_


Returns a list of source streams from which this stream collects data.
`
}

type infoTimestampCaller struct{}

func (caller infoTimestampCaller) Call(s *slip.Scope, args slip.List, _ int) slip.Object {
	self := s.Get("self").(*flavors.Instance)
	si := self.Any.(*jetstream.StreamInfo)

	return slip.Time(si.TimeStamp)
}

func (caller infoTimestampCaller) Docs() string {
	return `__:timestamp__ => _time_


Returns when the info was gathered by the server.
`
}

////////////////

type streamInfoCaller struct{}

func (caller streamInfoCaller) Call(s *slip.Scope, args slip.List, _ int) slip.Object {
	self := s.Get("self").(*flavors.Instance)
	slip.CheckMethodArgCount(self, ":info", len(args), 0, 6)
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
	return MakeStreamInfo(si)
}

func (caller streamInfoCaller) Docs() string {
	return `__:info__ &key _timeout_ _cached_ _deleted_ => _jet-stream-info_
   _:timeout_ [real] the number of seconds to wait before timing out.
   _:cached_ [boolean] return the cached information instead of fetching from the server.
   _:deleted_ [boolean] if true, include the information about messages deleted from a stream.


Returns the stream information as an instance of the _jet-stream-info_ flavor.
`
}
