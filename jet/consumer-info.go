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
	consumerInfoFlavor *flavors.Flavor
)

func defConsumerInfo() {
	consumerInfoFlavor = flavors.DefFlavor("jet-consumer-info",
		map[string]slip.Object{},
		[]string{},
		slip.List{
			slip.List{
				slip.Symbol(":documentation"),
				slip.String(`An instance with information about a consumer.`),
			},
		},
		&Pkg,
	)
	consumerInfoFlavor.Final = true
	consumerInfoFlavor.GoMakeOnly = true

	consumerInfoFlavor.DefMethod(":stream", "", consumerInfoStreamCaller{})
	flavors.FlosFun("jet-consumer-info-stream", ":stream", consumerInfoStreamCaller{}.Docs(), &Pkg)

	consumerInfoFlavor.DefMethod(":name", "", consumerInfoNameCaller{})
	flavors.FlosFun("jet-consumer-info-name", ":name", consumerInfoNameCaller{}.Docs(), &Pkg)

	consumerInfoFlavor.DefMethod(":created", "", consumerInfoCreatedCaller{})
	flavors.FlosFun("jet-consumer-info-created", ":created", consumerInfoCreatedCaller{}.Docs(), &Pkg)

	consumerInfoFlavor.DefMethod(":config", "", consumerInfoConfigCaller{})
	flavors.FlosFun("jet-consumer-info-config", ":config", consumerInfoConfigCaller{}.Docs(), &Pkg)

	consumerInfoFlavor.DefMethod(":delivered", "", consumerInfoDeliveredCaller{})
	flavors.FlosFun("jet-consumer-info-delivered", ":delivered", consumerInfoDeliveredCaller{}.Docs(), &Pkg)

	consumerInfoFlavor.DefMethod(":ack-floor", "", consumerInfoAckFloorCaller{})
	flavors.FlosFun("jet-consumer-info-ack-floor", ":ack-floor", consumerInfoAckFloorCaller{}.Docs(), &Pkg)

	consumerInfoFlavor.DefMethod(":number-ack-pending", "", consumerInfoNumberAckPendingCaller{})
	flavors.FlosFun("jet-consumer-info-number-ack-pending", ":number-ack-pending",
		consumerInfoNumberAckPendingCaller{}.Docs(), &Pkg)

	consumerInfoFlavor.DefMethod(":number-redelivered", "", consumerInfoNumberRedeliveredCaller{})
	flavors.FlosFun("jet-consumer-info-number-redelivered", ":number-redelivered",
		consumerInfoNumberRedeliveredCaller{}.Docs(), &Pkg)

	consumerInfoFlavor.DefMethod(":number-waiting", "", consumerInfoNumberWaitingCaller{})
	flavors.FlosFun("jet-consumer-info-number-waiting", ":number-waiting",
		consumerInfoNumberWaitingCaller{}.Docs(), &Pkg)

	consumerInfoFlavor.DefMethod(":number-pending", "", consumerInfoNumberPendingCaller{})
	flavors.FlosFun("jet-consumer-info-number-pending", ":number-pending",
		consumerInfoNumberPendingCaller{}.Docs(), &Pkg)

	consumerInfoFlavor.DefMethod(":cluster", "", consumerInfoClusterCaller{})
	flavors.FlosFun("jet-consumer-info-cluster", ":cluster", consumerInfoClusterCaller{}.Docs(), &Pkg)

	consumerInfoFlavor.DefMethod(":push-bound", "", consumerInfoPushBoundCaller{})
	flavors.FlosFun("jet-consumer-info-push-bound", ":push-bound", consumerInfoPushBoundCaller{}.Docs(), &Pkg)

	consumerInfoFlavor.DefMethod(":timestamp", "", consumerInfoTimestampCaller{})
	flavors.FlosFun("jet-consumer-info-timestamp", ":timestamp", consumerInfoTimestampCaller{}.Docs(), &Pkg)
}

// MakeConsumerInfo makes a jet-consumer-info.
func MakeConsumerInfo(info *jetstream.ConsumerInfo) (inst *flavors.Instance) {
	inst = consumerInfoFlavor.MakeInstance().(*flavors.Instance)
	inst.Any = info

	return
}

type consumerInfoStreamCaller struct{}

func (caller consumerInfoStreamCaller) Call(s *slip.Scope, args slip.List, _ int) slip.Object {
	self := s.Get("self").(*flavors.Instance)
	ci := self.Any.(*jetstream.ConsumerInfo)

	return slip.String(ci.Stream)
}

func (caller consumerInfoStreamCaller) Docs() string {
	return `__:stream__ => _string_


Returns the stream in the consumer-info.
`
}

type consumerInfoNameCaller struct{}

func (caller consumerInfoNameCaller) Call(s *slip.Scope, args slip.List, _ int) slip.Object {
	self := s.Get("self").(*flavors.Instance)
	ci := self.Any.(*jetstream.ConsumerInfo)

	return slip.String(ci.Name)
}

func (caller consumerInfoNameCaller) Docs() string {
	return `__:name__ => _string_


Returns the name in the consumer-info.
`
}

type consumerInfoCreatedCaller struct{}

func (caller consumerInfoCreatedCaller) Call(s *slip.Scope, args slip.List, _ int) slip.Object {
	self := s.Get("self").(*flavors.Instance)
	ci := self.Any.(*jetstream.ConsumerInfo)

	return slip.Time(ci.Created)
}

func (caller consumerInfoCreatedCaller) Docs() string {
	return `__:created__ => _time_


Returns the created in the consumer-info.
`
}

type consumerInfoConfigCaller struct{}

func (caller consumerInfoConfigCaller) Call(s *slip.Scope, args slip.List, _ int) slip.Object {
	self := s.Get("self").(*flavors.Instance)
	ci := self.Any.(*jetstream.ConsumerInfo)

	return ConsumerConfigPropList(&ci.Config)
}

func (caller consumerInfoConfigCaller) Docs() string {
	return `__:config__ => _property list_


Returns the config in the configuration in a consumer-info.
`
}

type consumerInfoDeliveredCaller struct{}

func (caller consumerInfoDeliveredCaller) Call(s *slip.Scope, args slip.List, _ int) slip.Object {
	self := s.Get("self").(*flavors.Instance)
	ci := self.Any.(*jetstream.ConsumerInfo)
	var last slip.Object
	if ci.Delivered.Last != nil {
		last = slip.Time(*ci.Delivered.Last)
	}
	return slip.List{
		slip.Symbol(":consumer"), slip.Fixnum(ci.Delivered.Consumer),
		slip.Symbol(":stream"), slip.Fixnum(ci.Delivered.Stream),
		slip.Symbol(":last"), last,
	}
}

func (caller consumerInfoDeliveredCaller) Docs() string {
	return `__:delivered__ => _property list_


Returns the delivered in the consumer-info.
`
}

type consumerInfoAckFloorCaller struct{}

func (caller consumerInfoAckFloorCaller) Call(s *slip.Scope, args slip.List, _ int) slip.Object {
	self := s.Get("self").(*flavors.Instance)
	ci := self.Any.(*jetstream.ConsumerInfo)
	var last slip.Object
	if ci.AckFloor.Last != nil {
		last = slip.Time(*ci.AckFloor.Last)
	}
	return slip.List{
		slip.Symbol(":consumer"), slip.Fixnum(ci.AckFloor.Consumer),
		slip.Symbol(":stream"), slip.Fixnum(ci.AckFloor.Stream),
		slip.Symbol(":last"), last,
	}
}

func (caller consumerInfoAckFloorCaller) Docs() string {
	return `__:ack-floor__ => _property list_


Returns the ack-floor in the consumer-info.
`
}

type consumerInfoNumberAckPendingCaller struct{}

func (caller consumerInfoNumberAckPendingCaller) Call(s *slip.Scope, args slip.List, _ int) slip.Object {
	self := s.Get("self").(*flavors.Instance)
	ci := self.Any.(*jetstream.ConsumerInfo)

	return slip.Fixnum(ci.NumAckPending)
}

func (caller consumerInfoNumberAckPendingCaller) Docs() string {
	return `__:number-ack-pending__ => _fixnum_


Returns the number-ack-pending in the consumer-info.
`
}

type consumerInfoNumberRedeliveredCaller struct{}

func (caller consumerInfoNumberRedeliveredCaller) Call(s *slip.Scope, args slip.List, _ int) slip.Object {
	self := s.Get("self").(*flavors.Instance)
	ci := self.Any.(*jetstream.ConsumerInfo)

	return slip.Fixnum(ci.NumRedelivered)
}

func (caller consumerInfoNumberRedeliveredCaller) Docs() string {
	return `__:number-redelivered__ => _fixnum_


Returns the number-redelivered in the consumer-info.
`
}

type consumerInfoNumberWaitingCaller struct{}

func (caller consumerInfoNumberWaitingCaller) Call(s *slip.Scope, args slip.List, _ int) slip.Object {
	self := s.Get("self").(*flavors.Instance)
	ci := self.Any.(*jetstream.ConsumerInfo)

	return slip.Fixnum(ci.NumWaiting)
}

func (caller consumerInfoNumberWaitingCaller) Docs() string {
	return `__:number-waiting__ => _fixnum_


Returns the number-waiting in the consumer-info.
`
}

type consumerInfoNumberPendingCaller struct{}

func (caller consumerInfoNumberPendingCaller) Call(s *slip.Scope, args slip.List, _ int) slip.Object {
	self := s.Get("self").(*flavors.Instance)
	ci := self.Any.(*jetstream.ConsumerInfo)

	return slip.Fixnum(ci.NumPending)
}

func (caller consumerInfoNumberPendingCaller) Docs() string {
	return `__:number-pending__ => _fixnum_


Returns the number-pending in the consumer-info.
`
}

type consumerInfoClusterCaller struct{}

func (caller consumerInfoClusterCaller) Call(s *slip.Scope, args slip.List, _ int) (result slip.Object) {
	self := s.Get("self").(*flavors.Instance)
	ci := self.Any.(*jetstream.ConsumerInfo)
	if ci.Cluster != nil {
		var replicas slip.List
		for _, pi := range ci.Cluster.Replicas {
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
			replicas = append(replicas,
				slip.List{
					slip.Symbol(":name"), slip.String(pi.Name),
					slip.Symbol(":current"), current,
					slip.Symbol(":offline"), offline,
					slip.Symbol(":active"), slip.DoubleFloat(float64(pi.Active) / float64(time.Second)),
					slip.Symbol(":lag"), slip.Fixnum(pi.Lag),
				})
		}
		result = slip.List{
			slip.Symbol(":name"), slip.String(ci.Cluster.Name),
			slip.Symbol(":leader"), slip.String(ci.Cluster.Leader),
			slip.Symbol(":replicas"), replicas,
		}
	}
	return
}

func (caller consumerInfoClusterCaller) Docs() string {
	return `__:cluster__ => _property list_


Returns the cluster in the consumer-info.
`
}

type consumerInfoPushBoundCaller struct{}

func (caller consumerInfoPushBoundCaller) Call(s *slip.Scope, args slip.List, _ int) (result slip.Object) {
	self := s.Get("self").(*flavors.Instance)
	ci := self.Any.(*jetstream.ConsumerInfo)
	if ci.PushBound {
		result = slip.True
	}
	return
}

func (caller consumerInfoPushBoundCaller) Docs() string {
	return `__:push-bound__ => _boolean_


Returns the push-bound in the consumer-info.
`
}

type consumerInfoTimestampCaller struct{}

func (caller consumerInfoTimestampCaller) Call(s *slip.Scope, args slip.List, _ int) slip.Object {
	self := s.Get("self").(*flavors.Instance)
	ci := self.Any.(*jetstream.ConsumerInfo)

	return slip.Time(ci.TimeStamp)
}

func (caller consumerInfoTimestampCaller) Docs() string {
	return `__:timestamp__ => _time_


Returns the timestamp in the consumer-info.
`
}

////////////////

type consumerInfoCaller struct{}

func (caller consumerInfoCaller) Call(s *slip.Scope, args slip.List, _ int) slip.Object {
	self := s.Get("self").(*flavors.Instance)
	slip.CheckMethodArgCount(self, ":info", len(args), 0, 6)
	consumer := self.Any.(jetstream.Consumer)
	var ci *jetstream.ConsumerInfo

	if v, has := slip.GetArgsKeyValue(args, slip.Symbol(":cached")); has && v != nil {
		ci = consumer.CachedInfo()
	} else {
		ctx := context.Background()
		if v, has := slip.GetArgsKeyValue(args, slip.Symbol(":timeout")); has {
			var cf context.CancelFunc
			ctx, cf = context.WithTimeout(ctx, mustBeDuration(v, ":timeout"))
			defer cf()
		}
		var err error
		if ci, err = consumer.Info(ctx); err != nil {
			panic(err)
		}
	}
	return MakeConsumerInfo(ci)
}

func (caller consumerInfoCaller) Docs() string {
	return `__:info__ &key _timeout_ _cached_ => _jet-consumer-info_
   _:timeout_ [real] the number of seconds to wait before timing out.
   _:cached_ [boolean] return the cached information instead of fetching from the server.


Returns the consumer information as an instance of the _jet-consumer-info_ flavor.
`
}
