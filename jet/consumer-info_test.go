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

// TBD func TestConsumerInfoMethod(t *testing.T) {
// TBD func TestConsumerInfoCached(t *testing.T) {

func TestConsumerInfoStream(t *testing.T) {
	tm := time.Date(2024, time.December, 9, 19, 00, 2, 123, time.UTC)
	info := sampleConsumerInfo(tm)

	scope := slip.NewScope()
	scope.Let(slip.Symbol("info"), jet.MakeConsumerInfo(info))
	(&sliptest.Function{
		Scope:  scope,
		Source: `(send info :stream)`,
		Expect: `"river"`,
	}).Test(t)
}

func TestConsumerInfoName(t *testing.T) {
	tm := time.Date(2024, time.December, 9, 19, 00, 2, 123, time.UTC)
	info := sampleConsumerInfo(tm)

	scope := slip.NewScope()
	scope.Let(slip.Symbol("info"), jet.MakeConsumerInfo(info))
	(&sliptest.Function{
		Scope:  scope,
		Source: `(send info :name)`,
		Expect: `"eater"`,
	}).Test(t)
}

func TestConsumerInfoCreated(t *testing.T) {
	tm := time.Date(2024, time.December, 9, 19, 00, 2, 123, time.UTC)
	info := sampleConsumerInfo(tm)

	scope := slip.NewScope()
	scope.Let(slip.Symbol("info"), jet.MakeConsumerInfo(info))
	(&sliptest.Function{
		Scope:  scope,
		Source: `(send info :created)`,
		Expect: "@2024-12-09T19:00:02.000000123Z",
	}).Test(t)
}

func TestConsumerInfoDelivered(t *testing.T) {
	tm := time.Date(2024, time.December, 9, 19, 00, 2, 123, time.UTC)
	info := sampleConsumerInfo(tm)

	scope := slip.NewScope()
	scope.Let(slip.Symbol("info"), jet.MakeConsumerInfo(info))
	(&sliptest.Function{
		Scope:  scope,
		Source: `(send info :delivered)`,
		Expect: `(:consumer 7 :stream 8 :last @2024-12-09T19:00:02.000000123Z)`,
	}).Test(t)
}

func TestConsumerInfoAckFloor(t *testing.T) {
	tm := time.Date(2024, time.December, 9, 19, 00, 2, 123, time.UTC)
	info := sampleConsumerInfo(tm)

	scope := slip.NewScope()
	scope.Let(slip.Symbol("info"), jet.MakeConsumerInfo(info))
	(&sliptest.Function{
		Scope:  scope,
		Source: `(send info :ack-floor)`,
		Expect: `(:consumer 3 :stream 4 :last @2024-12-09T19:00:02.000000123Z)`,
	}).Test(t)
}

func TestConsumerInfoNumAckPending(t *testing.T) {
	tm := time.Date(2024, time.December, 9, 19, 00, 2, 123, time.UTC)
	info := sampleConsumerInfo(tm)

	scope := slip.NewScope()
	scope.Let(slip.Symbol("info"), jet.MakeConsumerInfo(info))
	(&sliptest.Function{
		Scope:  scope,
		Source: `(send info :number-ack-pending)`,
		Expect: "2",
	}).Test(t)
}

func TestConsumerInfoNumRedelivered(t *testing.T) {
	tm := time.Date(2024, time.December, 9, 19, 00, 2, 123, time.UTC)
	info := sampleConsumerInfo(tm)

	scope := slip.NewScope()
	scope.Let(slip.Symbol("info"), jet.MakeConsumerInfo(info))
	(&sliptest.Function{
		Scope:  scope,
		Source: `(send info :number-redelivered)`,
		Expect: "5",
	}).Test(t)
}

func TestConsumerInfoNumWaiting(t *testing.T) {
	tm := time.Date(2024, time.December, 9, 19, 00, 2, 123, time.UTC)
	info := sampleConsumerInfo(tm)

	scope := slip.NewScope()
	scope.Let(slip.Symbol("info"), jet.MakeConsumerInfo(info))
	(&sliptest.Function{
		Scope:  scope,
		Source: `(send info :number-waiting)`,
		Expect: "1",
	}).Test(t)
}

func TestConsumerInfoNumPending(t *testing.T) {
	tm := time.Date(2024, time.December, 9, 19, 00, 2, 123, time.UTC)
	info := sampleConsumerInfo(tm)

	scope := slip.NewScope()
	scope.Let(slip.Symbol("info"), jet.MakeConsumerInfo(info))
	(&sliptest.Function{
		Scope:  scope,
		Source: `(send info :number-pending)`,
		Expect: "1",
	}).Test(t)
}

func TestConsumerInfoCluster(t *testing.T) {
	tm := time.Date(2024, time.December, 9, 19, 00, 2, 123, time.UTC)
	info := sampleConsumerInfo(tm)

	scope := slip.NewScope()
	scope.Let(slip.Symbol("info"), jet.MakeConsumerInfo(info))
	(&sliptest.Function{
		Scope:  scope,
		Source: `(send info :cluster)`,
		Expect: `(:name "kluster" :leader "ichiban" :replicas
       ((:name "peer" :current t :offline t :active 1 :lag 3)))`,
	}).Test(t)
}

func TestConsumerInfoPushBound(t *testing.T) {
	tm := time.Date(2024, time.December, 9, 19, 00, 2, 123, time.UTC)
	info := sampleConsumerInfo(tm)

	scope := slip.NewScope()
	scope.Let(slip.Symbol("info"), jet.MakeConsumerInfo(info))
	(&sliptest.Function{
		Scope:  scope,
		Source: `(send info :push-bound)`,
		Expect: "t",
	}).Test(t)
}

func TestConsumerInfoTimestamp(t *testing.T) {
	tm := time.Date(2024, time.December, 9, 19, 00, 2, 123, time.UTC)
	info := sampleConsumerInfo(tm)

	scope := slip.NewScope()
	scope.Let(slip.Symbol("info"), jet.MakeConsumerInfo(info))
	(&sliptest.Function{
		Scope:  scope,
		Source: `(send info :timestamp)`,
		Expect: "@2024-12-09T19:00:02.000000123Z",
	}).Test(t)
}

func sampleConsumerInfo(tm time.Time) *jetstream.ConsumerInfo {
	info := jetstream.ConsumerInfo{
		Stream:  "river",
		Name:    "eater",
		Created: tm,
		Delivered: jetstream.SequenceInfo{
			Consumer: 7,
			Stream:   8,
			Last:     &tm,
		},
		AckFloor: jetstream.SequenceInfo{
			Consumer: 3,
			Stream:   4,
			Last:     &tm,
		},
		NumAckPending:  2,
		NumRedelivered: 5,
		NumWaiting:     1,
		NumPending:     1,
		Cluster: &jetstream.ClusterInfo{
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
		},
		PushBound: true,
		TimeStamp: tm,
	}
	sampleConsumerConfig(&info.Config)

	return &info
}
