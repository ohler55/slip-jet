// Copyright (c) 2024, Peter Ohler, All rights reserved.

package jet_test

import (
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"
	"github.com/ohler55/slip"
	"github.com/ohler55/slip-jet/jet"
	"github.com/ohler55/slip/sliptest"
)

func TestConsumerInfoStream(t *testing.T) {
	tm := time.Date(2024, time.December, 9, 19, 00, 2, 123, time.UTC)
	var info jetstream.ConsumerInfo
	sampleConsumerInfo(&info, tm)

	scope := slip.NewScope()
	scope.Let(slip.Symbol("info"), jet.MakeConsumerInfo(&info))
	(&sliptest.Function{
		Scope:  scope,
		Source: `(send info :stream)`,
		Expect: `"river"`,
	}).Test(t)
}

func TestConsumerInfoName(t *testing.T) {
	tm := time.Date(2024, time.December, 9, 19, 00, 2, 123, time.UTC)
	var info jetstream.ConsumerInfo
	sampleConsumerInfo(&info, tm)

	scope := slip.NewScope()
	scope.Let(slip.Symbol("info"), jet.MakeConsumerInfo(&info))
	(&sliptest.Function{
		Scope:  scope,
		Source: `(send info :name)`,
		Expect: `"eater"`,
	}).Test(t)
}

func TestConsumerInfoCreated(t *testing.T) {
	tm := time.Date(2024, time.December, 9, 19, 00, 2, 123, time.UTC)
	var info jetstream.ConsumerInfo
	sampleConsumerInfo(&info, tm)

	scope := slip.NewScope()
	scope.Let(slip.Symbol("info"), jet.MakeConsumerInfo(&info))
	(&sliptest.Function{
		Scope:  scope,
		Source: `(send info :created)`,
		Expect: "@2024-12-09T19:00:02.000000123Z",
	}).Test(t)
}

func TestConsumerInfoDelivered(t *testing.T) {
	tm := time.Date(2024, time.December, 9, 19, 00, 2, 123, time.UTC)
	var info jetstream.ConsumerInfo
	sampleConsumerInfo(&info, tm)

	scope := slip.NewScope()
	scope.Let(slip.Symbol("info"), jet.MakeConsumerInfo(&info))
	(&sliptest.Function{
		Scope:  scope,
		Source: `(send info :delivered)`,
		Expect: `(:consumer 7 :stream 8 :last @2024-12-09T19:00:02.000000123Z)`,
	}).Test(t)
}

func TestConsumerInfoAckFloor(t *testing.T) {
	tm := time.Date(2024, time.December, 9, 19, 00, 2, 123, time.UTC)
	var info jetstream.ConsumerInfo
	sampleConsumerInfo(&info, tm)

	scope := slip.NewScope()
	scope.Let(slip.Symbol("info"), jet.MakeConsumerInfo(&info))
	(&sliptest.Function{
		Scope:  scope,
		Source: `(send info :ack-floor)`,
		Expect: `(:consumer 3 :stream 4 :last @2024-12-09T19:00:02.000000123Z)`,
	}).Test(t)
}

func TestConsumerInfoNumAckPending(t *testing.T) {
	tm := time.Date(2024, time.December, 9, 19, 00, 2, 123, time.UTC)
	var info jetstream.ConsumerInfo
	sampleConsumerInfo(&info, tm)

	scope := slip.NewScope()
	scope.Let(slip.Symbol("info"), jet.MakeConsumerInfo(&info))
	(&sliptest.Function{
		Scope:  scope,
		Source: `(send info :number-ack-pending)`,
		Expect: "2",
	}).Test(t)
}

func TestConsumerInfoNumRedelivered(t *testing.T) {
	tm := time.Date(2024, time.December, 9, 19, 00, 2, 123, time.UTC)
	var info jetstream.ConsumerInfo
	sampleConsumerInfo(&info, tm)

	scope := slip.NewScope()
	scope.Let(slip.Symbol("info"), jet.MakeConsumerInfo(&info))
	(&sliptest.Function{
		Scope:  scope,
		Source: `(send info :number-redelivered)`,
		Expect: "5",
	}).Test(t)
}

func TestConsumerInfoNumWaiting(t *testing.T) {
	tm := time.Date(2024, time.December, 9, 19, 00, 2, 123, time.UTC)
	var info jetstream.ConsumerInfo
	sampleConsumerInfo(&info, tm)

	scope := slip.NewScope()
	scope.Let(slip.Symbol("info"), jet.MakeConsumerInfo(&info))
	(&sliptest.Function{
		Scope:  scope,
		Source: `(send info :number-waiting)`,
		Expect: "1",
	}).Test(t)
}

func TestConsumerInfoNumPending(t *testing.T) {
	tm := time.Date(2024, time.December, 9, 19, 00, 2, 123, time.UTC)
	var info jetstream.ConsumerInfo
	sampleConsumerInfo(&info, tm)

	scope := slip.NewScope()
	scope.Let(slip.Symbol("info"), jet.MakeConsumerInfo(&info))
	(&sliptest.Function{
		Scope:  scope,
		Source: `(send info :number-pending)`,
		Expect: "1",
	}).Test(t)
}

func TestConsumerInfoCluster(t *testing.T) {
	tm := time.Date(2024, time.December, 9, 19, 00, 2, 123, time.UTC)
	var info jetstream.ConsumerInfo
	sampleConsumerInfo(&info, tm)

	scope := slip.NewScope()
	scope.Let(slip.Symbol("info"), jet.MakeConsumerInfo(&info))
	(&sliptest.Function{
		Scope:  scope,
		Source: `(send info :cluster)`,
		Expect: `(:name "kluster" :leader "ichiban" :replicas
       ((:name "peer" :current t :offline t :active 1 :lag 3)))`,
	}).Test(t)
}

func TestConsumerInfoPushBound(t *testing.T) {
	tm := time.Date(2024, time.December, 9, 19, 00, 2, 123, time.UTC)
	var info jetstream.ConsumerInfo
	sampleConsumerInfo(&info, tm)

	scope := slip.NewScope()
	scope.Let(slip.Symbol("info"), jet.MakeConsumerInfo(&info))
	(&sliptest.Function{
		Scope:  scope,
		Source: `(send info :push-bound)`,
		Expect: "t",
	}).Test(t)
}

func TestConsumerInfoTimestamp(t *testing.T) {
	tm := time.Date(2024, time.December, 9, 19, 00, 2, 123, time.UTC)
	var info jetstream.ConsumerInfo
	sampleConsumerInfo(&info, tm)

	scope := slip.NewScope()
	scope.Let(slip.Symbol("info"), jet.MakeConsumerInfo(&info))
	(&sliptest.Function{
		Scope:  scope,
		Source: `(send info :timestamp)`,
		Expect: "@2024-12-09T19:00:02.000000123Z",
	}).Test(t)
}

func TestConsumerInfoPaused(t *testing.T) {
	tm := time.Date(2024, time.December, 9, 19, 00, 2, 123, time.UTC)
	var info jetstream.ConsumerInfo
	sampleConsumerInfo(&info, tm)

	scope := slip.NewScope()
	scope.Let(slip.Symbol("info"), jet.MakeConsumerInfo(&info))
	(&sliptest.Function{
		Scope:  scope,
		Source: `(send info :paused)`,
		Expect: "t",
	}).Test(t)
}

func TestConsumerInfoPauseRemaining(t *testing.T) {
	tm := time.Date(2024, time.December, 9, 19, 00, 2, 123, time.UTC)
	var info jetstream.ConsumerInfo
	sampleConsumerInfo(&info, tm)

	scope := slip.NewScope()
	scope.Let(slip.Symbol("info"), jet.MakeConsumerInfo(&info))
	(&sliptest.Function{
		Scope:  scope,
		Source: `(send info :pause-remaining)`,
		Expect: "1.2",
	}).Test(t)
}

func TestConsumerInfoPriorityGroups(t *testing.T) {
	tm := time.Date(2024, time.December, 9, 19, 00, 2, 123, time.UTC)
	var info jetstream.ConsumerInfo
	sampleConsumerInfo(&info, tm)

	scope := slip.NewScope()
	scope.Let(slip.Symbol("info"), jet.MakeConsumerInfo(&info))
	(&sliptest.Function{
		Scope:  scope,
		Source: `(send info :priority-groups)`,
		Expect: `/\(\(:group "grouper" :pinned-client-id "clide" :pinned-timestamp/`,
	}).Test(t)
}

func sampleConsumerInfo(info *jetstream.ConsumerInfo, tm time.Time) {
	info.Stream = "river"
	info.Name = "eater"
	info.Created = tm
	info.Delivered = jetstream.SequenceInfo{
		Consumer: 7,
		Stream:   8,
		Last:     &tm,
	}
	info.AckFloor = jetstream.SequenceInfo{
		Consumer: 3,
		Stream:   4,
		Last:     &tm,
	}
	info.NumAckPending = 2
	info.NumRedelivered = 5
	info.NumWaiting = 1
	info.NumPending = 1
	info.Cluster = &jetstream.ClusterInfo{
		Name:   "kluster",
		Leader: "ichiban",
		Replicas: []*jetstream.PeerInfo{
			{
				Name:    "peer",
				Current: true,
				Offline: true,
				Active:  time.Second,
				Lag:     3,
			},
		},
	}
	info.PushBound = true
	info.TimeStamp = tm
	info.Paused = true
	info.PauseRemaining = time.Second + time.Millisecond*200
	info.PriorityGroups = []jetstream.PriorityGroupState{
		{Group: "grouper", PinnedClientID: "clide", PinnedTS: tm},
	}
	sampleConsumerConfig(&info.Config)
}
