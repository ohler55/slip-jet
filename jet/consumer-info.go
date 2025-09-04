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
	// consumerInfoFlavor.GoMakeOnly = true

	consumerInfoFlavor.DefMethod(":stream", "", consumerInfoStreamCaller{})
	flavors.FlosFun("jet-consumer-info-stream", ":stream", consumerInfoStreamCaller{}.FuncDocs(), &Pkg)

	consumerInfoFlavor.DefMethod(":name", "", consumerInfoNameCaller{})
	flavors.FlosFun("jet-consumer-info-name", ":name", consumerInfoNameCaller{}.FuncDocs(), &Pkg)

	consumerInfoFlavor.DefMethod(":created", "", consumerInfoCreatedCaller{})
	flavors.FlosFun("jet-consumer-info-created", ":created", consumerInfoCreatedCaller{}.FuncDocs(), &Pkg)

	consumerInfoFlavor.DefMethod(":config", "", consumerInfoConfigCaller{})
	flavors.FlosFun("jet-consumer-info-config", ":config", consumerInfoConfigCaller{}.FuncDocs(), &Pkg)

	consumerInfoFlavor.DefMethod(":delivered", "", consumerInfoDeliveredCaller{})
	flavors.FlosFun("jet-consumer-info-delivered", ":delivered", consumerInfoDeliveredCaller{}.FuncDocs(), &Pkg)

	consumerInfoFlavor.DefMethod(":ack-floor", "", consumerInfoAckFloorCaller{})
	flavors.FlosFun("jet-consumer-info-ack-floor", ":ack-floor", consumerInfoAckFloorCaller{}.FuncDocs(), &Pkg)

	consumerInfoFlavor.DefMethod(":number-ack-pending", "", consumerInfoNumberAckPendingCaller{})
	flavors.FlosFun("jet-consumer-info-number-ack-pending", ":number-ack-pending",
		consumerInfoNumberAckPendingCaller{}.FuncDocs(), &Pkg)

	consumerInfoFlavor.DefMethod(":number-redelivered", "", consumerInfoNumberRedeliveredCaller{})
	flavors.FlosFun("jet-consumer-info-number-redelivered", ":number-redelivered",
		consumerInfoNumberRedeliveredCaller{}.FuncDocs(), &Pkg)

	consumerInfoFlavor.DefMethod(":number-waiting", "", consumerInfoNumberWaitingCaller{})
	flavors.FlosFun("jet-consumer-info-number-waiting", ":number-waiting",
		consumerInfoNumberWaitingCaller{}.FuncDocs(), &Pkg)

	consumerInfoFlavor.DefMethod(":number-pending", "", consumerInfoNumberPendingCaller{})
	flavors.FlosFun("jet-consumer-info-number-pending", ":number-pending",
		consumerInfoNumberPendingCaller{}.FuncDocs(), &Pkg)

	consumerInfoFlavor.DefMethod(":cluster", "", consumerInfoClusterCaller{})
	flavors.FlosFun("jet-consumer-info-cluster", ":cluster", consumerInfoClusterCaller{}.FuncDocs(), &Pkg)

	consumerInfoFlavor.DefMethod(":push-bound", "", consumerInfoPushBoundCaller{})
	flavors.FlosFun("jet-consumer-info-push-bound", ":push-bound", consumerInfoPushBoundCaller{}.FuncDocs(), &Pkg)

	consumerInfoFlavor.DefMethod(":timestamp", "", consumerInfoTimestampCaller{})
	flavors.FlosFun("jet-consumer-info-timestamp", ":timestamp", consumerInfoTimestampCaller{}.FuncDocs(), &Pkg)

	consumerInfoFlavor.DefMethod(":paused", "", consumerInfoPausedCaller{})
	flavors.FlosFun("jet-consumer-info-paused", ":paused", consumerInfoPausedCaller{}.FuncDocs(), &Pkg)

	consumerInfoFlavor.DefMethod(":pause-remaining", "", consumerInfoPauseRemainingCaller{})
	flavors.FlosFun("jet-consumer-info-pause-remaining", ":pause-remaining",
		consumerInfoPauseRemainingCaller{}.FuncDocs(), &Pkg)

	consumerInfoFlavor.DefMethod(":priority-groups", "", consumerInfoPriorityGroupsCaller{})
	flavors.FlosFun("jet-consumer-info-priority-groups", ":priorityGroups",
		consumerInfoPriorityGroupsCaller{}.FuncDocs(), &Pkg)
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

func (caller consumerInfoStreamCaller) FuncDocs() *slip.FuncDoc {
	return &slip.FuncDoc{
		Name:   ":stream",
		Text:   `Returns the stream in the consumer-info.`,
		Return: "string",
	}
}

type consumerInfoNameCaller struct{}

func (caller consumerInfoNameCaller) Call(s *slip.Scope, args slip.List, _ int) slip.Object {
	self := s.Get("self").(*flavors.Instance)
	ci := self.Any.(*jetstream.ConsumerInfo)

	return slip.String(ci.Name)
}

func (caller consumerInfoNameCaller) FuncDocs() *slip.FuncDoc {
	return &slip.FuncDoc{
		Name:   ":name",
		Text:   `Returns the name in the consumer-info.`,
		Return: "string",
	}
}

type consumerInfoCreatedCaller struct{}

func (caller consumerInfoCreatedCaller) Call(s *slip.Scope, args slip.List, _ int) slip.Object {
	self := s.Get("self").(*flavors.Instance)
	ci := self.Any.(*jetstream.ConsumerInfo)

	return slip.Time(ci.Created)
}

func (caller consumerInfoCreatedCaller) FuncDocs() *slip.FuncDoc {
	return &slip.FuncDoc{
		Name:   ":created",
		Text:   `Returns the created in the consumer-info.`,
		Return: "time",
	}
}

type consumerInfoConfigCaller struct{}

func (caller consumerInfoConfigCaller) Call(s *slip.Scope, args slip.List, _ int) slip.Object {
	self := s.Get("self").(*flavors.Instance)
	ci := self.Any.(*jetstream.ConsumerInfo)

	return ConsumerConfigPropList(&ci.Config)
}

func (caller consumerInfoConfigCaller) FuncDocs() *slip.FuncDoc {
	return &slip.FuncDoc{
		Name:   ":config",
		Text:   `Returns the config in the configuration in a consumer-info.`,
		Return: "property-list",
	}
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

func (caller consumerInfoDeliveredCaller) FuncDocs() *slip.FuncDoc {
	return &slip.FuncDoc{
		Name:   ":delivered",
		Text:   `Returns the delivered in the consumer-info.`,
		Return: "property-list",
	}
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

func (caller consumerInfoAckFloorCaller) FuncDocs() *slip.FuncDoc {
	return &slip.FuncDoc{
		Name:   ":ack-floor",
		Text:   `Returns the ack-floor in the consumer-info.`,
		Return: "property-list",
	}
}

type consumerInfoNumberAckPendingCaller struct{}

func (caller consumerInfoNumberAckPendingCaller) Call(s *slip.Scope, args slip.List, _ int) slip.Object {
	self := s.Get("self").(*flavors.Instance)
	ci := self.Any.(*jetstream.ConsumerInfo)

	return slip.Fixnum(ci.NumAckPending)
}

func (caller consumerInfoNumberAckPendingCaller) FuncDocs() *slip.FuncDoc {
	return &slip.FuncDoc{
		Name:   ":number-ack-pending",
		Text:   `Returns the number-ack-pending in the consumer-info.`,
		Return: "fixnum",
	}
}

type consumerInfoNumberRedeliveredCaller struct{}

func (caller consumerInfoNumberRedeliveredCaller) Call(s *slip.Scope, args slip.List, _ int) slip.Object {
	self := s.Get("self").(*flavors.Instance)
	ci := self.Any.(*jetstream.ConsumerInfo)

	return slip.Fixnum(ci.NumRedelivered)
}

func (caller consumerInfoNumberRedeliveredCaller) FuncDocs() *slip.FuncDoc {
	return &slip.FuncDoc{
		Name:   ":number-redelivered",
		Text:   `Returns the number-redelivered in the consumer-info.`,
		Return: "fixnum",
	}
}

type consumerInfoNumberWaitingCaller struct{}

func (caller consumerInfoNumberWaitingCaller) Call(s *slip.Scope, args slip.List, _ int) slip.Object {
	self := s.Get("self").(*flavors.Instance)
	ci := self.Any.(*jetstream.ConsumerInfo)

	return slip.Fixnum(ci.NumWaiting)
}

func (caller consumerInfoNumberWaitingCaller) FuncDocs() *slip.FuncDoc {
	return &slip.FuncDoc{
		Name:   ":number-waiting",
		Text:   `Returns the number-waiting in the consumer-info.`,
		Return: "fixnum",
	}
}

type consumerInfoNumberPendingCaller struct{}

func (caller consumerInfoNumberPendingCaller) Call(s *slip.Scope, args slip.List, _ int) slip.Object {
	self := s.Get("self").(*flavors.Instance)
	ci := self.Any.(*jetstream.ConsumerInfo)

	return slip.Fixnum(ci.NumPending)
}

func (caller consumerInfoNumberPendingCaller) FuncDocs() *slip.FuncDoc {
	return &slip.FuncDoc{
		Name:   ":number-pending",
		Text:   `Returns the number-pending in the consumer-info.`,
		Return: "fixnum",
	}
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

func (caller consumerInfoClusterCaller) FuncDocs() *slip.FuncDoc {
	return &slip.FuncDoc{
		Name:   ":cluster",
		Text:   `Returns the cluster in the consumer-info.`,
		Return: "property-list",
	}
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

func (caller consumerInfoPushBoundCaller) FuncDocs() *slip.FuncDoc {
	return &slip.FuncDoc{
		Name:   ":push-bound",
		Text:   `Returns the push-bound in the consumer-info.`,
		Return: "boolean",
	}
}

type consumerInfoTimestampCaller struct{}

func (caller consumerInfoTimestampCaller) Call(s *slip.Scope, args slip.List, _ int) slip.Object {
	self := s.Get("self").(*flavors.Instance)
	ci := self.Any.(*jetstream.ConsumerInfo)

	return slip.Time(ci.TimeStamp)
}

func (caller consumerInfoTimestampCaller) FuncDocs() *slip.FuncDoc {
	return &slip.FuncDoc{
		Name:   ":timestamp",
		Text:   `Returns the timestamp in the consumer-info.`,
		Return: "time",
	}
}

type consumerInfoPausedCaller struct{}

func (caller consumerInfoPausedCaller) Call(s *slip.Scope, args slip.List, _ int) (result slip.Object) {
	self := s.Get("self").(*flavors.Instance)
	ci := self.Any.(*jetstream.ConsumerInfo)
	if ci.Paused {
		result = slip.True
	}
	return
}

func (caller consumerInfoPausedCaller) FuncDocs() *slip.FuncDoc {
	return &slip.FuncDoc{
		Name:   ":paused",
		Text:   `Returns the paused indicator of the consumer-info.`,
		Return: "boolean",
	}
}

type consumerInfoPauseRemainingCaller struct{}

func (caller consumerInfoPauseRemainingCaller) Call(s *slip.Scope, args slip.List, _ int) slip.Object {
	self := s.Get("self").(*flavors.Instance)
	ci := self.Any.(*jetstream.ConsumerInfo)

	return slip.DoubleFloat(float64(ci.PauseRemaining) / float64(time.Second))
}

func (caller consumerInfoPauseRemainingCaller) FuncDocs() *slip.FuncDoc {
	return &slip.FuncDoc{
		Name:   ":pause-remaining",
		Text:   `Returns the pause remaining in the consumer-info in seconds.`,
		Return: "real",
	}
}

type consumerInfoPriorityGroupsCaller struct{}

func (caller consumerInfoPriorityGroupsCaller) Call(s *slip.Scope, args slip.List, _ int) slip.Object {
	self := s.Get("self").(*flavors.Instance)
	ci := self.Any.(*jetstream.ConsumerInfo)
	var groups slip.List
	for _, pg := range ci.PriorityGroups {
		groups = append(groups, slip.List{
			slip.Symbol(":group"), slip.String(pg.Group),
			slip.Symbol(":pinned-client-id"), slip.String(pg.PinnedClientID),
			slip.Symbol(":pinned-timestamp"), slip.Time(pg.PinnedTS),
		})
	}
	return groups
}

func (caller consumerInfoPriorityGroupsCaller) FuncDocs() *slip.FuncDoc {
	return &slip.FuncDoc{
		Name:   ":priority-groups",
		Text:   `Returns the priority groups of the consumer-info.`,
		Return: "boolean",
	}
}

////////////////

type consumerInfoCaller struct{}

func (caller consumerInfoCaller) Call(s *slip.Scope, args slip.List, depth int) slip.Object {
	self := s.Get("self").(*flavors.Instance)
	slip.MethodArgCountCheck(s, depth, self, ":info", len(args), 0, 6)
	consumer := self.Any.(jetstream.Consumer)
	var ci *jetstream.ConsumerInfo

	if v, has := slip.GetArgsKeyValue(args, slip.Symbol(":cached")); has && v != nil {
		ci = consumer.CachedInfo()
	} else {
		ctx := context.Background()
		if v, has := slip.GetArgsKeyValue(args, slip.Symbol(":timeout")); has {
			var cf context.CancelFunc
			ctx, cf = context.WithTimeout(ctx, mustBeDuration(s, v, ":timeout", depth))
			defer cf()
		}
		var err error
		if ci, err = consumer.Info(ctx); err != nil {
			panic(err)
		}
	}
	return MakeConsumerInfo(ci)
}

func (caller consumerInfoCaller) FuncDocs() *slip.FuncDoc {
	return &slip.FuncDoc{
		Name: ":info",
		Text: `Returns the consumer information as an instance of the _jet-consumer-info_ flavor.`,
		Args: []*slip.DocArg{
			{Name: "&key"},
			{
				Name: ":timeout",
				Type: "real",
				Text: "The number of seconds to wait before timing out.",
			},
			{
				Name: ":cached",
				Type: "boolean",
				Text: "Return the cached information instead of fetching from the server.",
			},
		},
		Return: "_jet-consumer-info",
	}
}
