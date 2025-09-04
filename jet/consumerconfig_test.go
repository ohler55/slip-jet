// Copyright (c) 2024, Peter Ohler, All rights reserved.

package jet_test

import (
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"
	"github.com/ohler55/ojg/tt"
	"github.com/ohler55/slip"
	"github.com/ohler55/slip-jet/jet"
)

func TestConsumerConfigOk(t *testing.T) {
	var cfg jetstream.ConsumerConfig

	sampleConsumerConfig(&cfg)

	plist := jet.ConsumerConfigPropList(&cfg)

	checkPlistValue(t, ":name", plist, slip.String("river"))
	checkPlistValue(t, ":durable", plist, slip.String("dur"))
	checkPlistValue(t, ":description", plist, slip.String("description"))
	checkPlistValue(t, ":deliver-policy", plist, slip.Symbol(":last"))
	checkPlistValue(t, ":opt-start-seq", plist, slip.Fixnum(3))
	checkPlistValue(t, ":opt-start-time", plist,
		slip.Time(time.Date(2024, time.December, 9, 19, 18, 17, 16, time.UTC)))
	checkPlistValue(t, ":ack-policy", plist, slip.Symbol(":explicit"))
	checkPlistValue(t, ":ack-wait", plist, slip.DoubleFloat(1.5))
	checkPlistValue(t, ":max-deliver", plist, slip.Fixnum(2))
	checkPlistValue(t, ":back-off", plist, slip.List{slip.DoubleFloat(0.5), slip.DoubleFloat(1)})
	checkPlistValue(t, ":filter-subject", plist, slip.String("test.ignore"))
	checkPlistValue(t, ":replay-policy", plist, slip.Symbol(":instant"))
	checkPlistValue(t, ":rate-limit", plist, slip.Fixnum(100))
	checkPlistValue(t, ":sample-frequency", plist, slip.String("10Hz"))
	checkPlistValue(t, ":max-waiting", plist, slip.Fixnum(5))
	checkPlistValue(t, ":max-ack-pending", plist, slip.Fixnum(4))
	checkPlistValue(t, ":headers-only", plist, slip.True)
	checkPlistValue(t, ":max-request-batch", plist, slip.Fixnum(3))
	checkPlistValue(t, ":max-request-expires", plist, slip.DoubleFloat(2.5))
	checkPlistValue(t, ":max-request-max-bytes", plist, slip.Fixnum(5000))
	checkPlistValue(t, ":inactive-threshold", plist, slip.DoubleFloat(4.5))
	checkPlistValue(t, ":replicas", plist, slip.Fixnum(1))
	checkPlistValue(t, ":memory-storage", plist, slip.True)
	checkPlistValue(t, ":filter-subjects", plist, slip.List{slip.String("test.one"), slip.String("test.two")})
	checkPlistValue(t, ":metadata", plist, slip.List{slip.String("meta"), slip.String("data")})
	checkPlistValue(t, ":pause-until", plist, slip.Time(time.Date(2024, time.December, 9, 19, 18, 17, 16, time.UTC)))
	checkPlistValue(t, ":priority-policy", plist, slip.Symbol(":pinned"))
	checkPlistValue(t, ":pinned-ttl", plist, slip.DoubleFloat(2.5))
	checkPlistValue(t, ":priority-groups", plist, slip.List{slip.String("quux")})
	checkPlistValue(t, ":deliver-subject", plist, slip.String("test.deliver"))
	checkPlistValue(t, ":deliver-group", plist, slip.String("quux"))
	checkPlistValue(t, ":flow-control", plist, slip.True)
	checkPlistValue(t, ":idle-heartbeat", plist, slip.DoubleFloat(7.5))
}

func sampleConsumerConfig(cfg *jetstream.ConsumerConfig) {
	jet.InitConsumerConfig(cfg, slip.List{
		slip.Symbol(":name"), slip.String("river"),
		slip.Symbol(":durable"), slip.String("dur"),
		slip.Symbol(":description"), slip.String("description"),
		slip.Symbol(":deliver-policy"), slip.Symbol(":last"),
		slip.Symbol(":opt-start-seq"), slip.Fixnum(3),
		slip.Symbol(":opt-start-time"), slip.Time(time.Date(2024, time.December, 9, 19, 18, 17, 16, time.UTC)),
		slip.Symbol(":ack-policy"), slip.Symbol(":explicit"),
		slip.Symbol(":ack-wait"), slip.DoubleFloat(1.5),
		slip.Symbol(":max-deliver"), slip.Fixnum(2),
		slip.Symbol(":back-off"), slip.List{slip.DoubleFloat(0.5), slip.Fixnum(1)},
		slip.Symbol(":filter-subject"), slip.String("test.ignore"),
		slip.Symbol(":replay-policy"), slip.Symbol(":instant"),
		slip.Symbol(":rate-limit"), slip.Fixnum(100),
		slip.Symbol(":sample-frequency"), slip.String("10Hz"),
		slip.Symbol(":max-waiting"), slip.Fixnum(5),
		slip.Symbol(":max-ack-pending"), slip.Fixnum(4),
		slip.Symbol(":headers-only"), slip.True,
		slip.Symbol(":max-request-batch"), slip.Fixnum(3),
		slip.Symbol(":max-request-expires"), slip.DoubleFloat(2.5),
		slip.Symbol(":max-request-max-bytes"), slip.Fixnum(5000),
		slip.Symbol(":inactive-threshold"), slip.DoubleFloat(4.5),
		slip.Symbol(":replicas"), slip.Fixnum(1),
		slip.Symbol(":memory-storage"), slip.True,
		slip.Symbol(":filter-subjects"), slip.List{slip.String("test.one"), slip.String("test.two")},
		slip.Symbol(":metadata"), slip.List{slip.String("meta"), slip.String("data")},
		slip.Symbol(":pause-until"), slip.Time(time.Date(2024, time.December, 9, 19, 18, 17, 16, time.UTC)),
		slip.Symbol(":priority-policy"), slip.Symbol(":pinned"),
		slip.Symbol(":pinned-ttl"), slip.DoubleFloat(2.5),
		slip.Symbol(":priority-groups"), slip.List{slip.String("quux")},
		slip.Symbol(":deliver-subject"), slip.String("test.deliver"),
		slip.Symbol(":deliver-group"), slip.String("quux"),
		slip.Symbol(":flow-control"), slip.True,
		slip.Symbol(":idle-heartbeat"), slip.DoubleFloat(7.5),
	})
}

func TestConsumerConfigDeliverPolicy(t *testing.T) {
	var cfg jetstream.ConsumerConfig
	tt.Panic(t, func() {
		jet.InitConsumerConfig(&cfg, slip.List{slip.Symbol(":deliver-policy"), slip.True})
	})
}

func TestConsumerConfigOptStartSeq(t *testing.T) {
	var cfg jetstream.ConsumerConfig
	tt.Panic(t, func() {
		jet.InitConsumerConfig(&cfg, slip.List{slip.Symbol(":opt-start-seq"), slip.True})
	})
}

func TestConsumerConfigOptStartTime(t *testing.T) {
	var cfg jetstream.ConsumerConfig
	tt.Panic(t, func() {
		jet.InitConsumerConfig(&cfg, slip.List{slip.Symbol(":opt-start-time"), slip.True})
	})
}

func TestConsumerConfigAckPolicy(t *testing.T) {
	var cfg jetstream.ConsumerConfig
	tt.Panic(t, func() {
		jet.InitConsumerConfig(&cfg, slip.List{slip.Symbol(":ack-policy"), slip.True})
	})
}

func TestConsumerConfigAckWait(t *testing.T) {
	var cfg jetstream.ConsumerConfig
	tt.Panic(t, func() {
		jet.InitConsumerConfig(&cfg, slip.List{slip.Symbol(":ack-wait"), slip.True})
	})
}

func TestConsumerConfigMaxDeliver(t *testing.T) {
	var cfg jetstream.ConsumerConfig
	tt.Panic(t, func() {
		jet.InitConsumerConfig(&cfg, slip.List{slip.Symbol(":max-deliver"), slip.True})
	})
}

func TestConsumerConfigBackOff(t *testing.T) {
	var cfg jetstream.ConsumerConfig
	tt.Panic(t, func() {
		jet.InitConsumerConfig(&cfg, slip.List{slip.Symbol(":back-off"), slip.True})
	})
	tt.Panic(t, func() {
		jet.InitConsumerConfig(&cfg, slip.List{slip.Symbol(":back-off"), slip.List{slip.True}})
	})
}

func TestConsumerConfigReplayPolicy(t *testing.T) {
	var cfg jetstream.ConsumerConfig
	jet.InitConsumerConfig(&cfg, slip.List{
		slip.Symbol(":replay-policy"), slip.Symbol(":original"),
	})
	plist := jet.ConsumerConfigPropList(&cfg)
	checkPlistValue(t, ":replay-policy", plist, slip.Symbol(":original"))

	tt.Panic(t, func() {
		jet.InitConsumerConfig(&cfg, slip.List{slip.Symbol(":replay-policy"), slip.True})
	})
}

func TestConsumerConfigRateLimit(t *testing.T) {
	var cfg jetstream.ConsumerConfig
	tt.Panic(t, func() {
		jet.InitConsumerConfig(&cfg, slip.List{slip.Symbol(":rate-limit"), slip.True})
	})
}

func TestConsumerConfigMaxWaiting(t *testing.T) {
	var cfg jetstream.ConsumerConfig
	tt.Panic(t, func() {
		jet.InitConsumerConfig(&cfg, slip.List{slip.Symbol(":max-waiting"), slip.True})
	})
}

func TestConsumerConfigMaxAckPending(t *testing.T) {
	var cfg jetstream.ConsumerConfig
	tt.Panic(t, func() {
		jet.InitConsumerConfig(&cfg, slip.List{slip.Symbol(":max-ack-pending"), slip.True})
	})
}

func TestConsumerConfigMaxRequestBatch(t *testing.T) {
	var cfg jetstream.ConsumerConfig
	tt.Panic(t, func() {
		jet.InitConsumerConfig(&cfg, slip.List{slip.Symbol(":max-request-batch"), slip.True})
	})
}

func TestConsumerConfigMaxRequestExpires(t *testing.T) {
	var cfg jetstream.ConsumerConfig
	tt.Panic(t, func() {
		jet.InitConsumerConfig(&cfg, slip.List{slip.Symbol(":max-request-expires"), slip.True})
	})
}

func TestConsumerConfigMaxRequestMaxBytes(t *testing.T) {
	var cfg jetstream.ConsumerConfig
	tt.Panic(t, func() {
		jet.InitConsumerConfig(&cfg, slip.List{slip.Symbol(":max-request-max-bytes"), slip.True})
	})
}

func TestConsumerConfigInactiveThreshold(t *testing.T) {
	var cfg jetstream.ConsumerConfig
	tt.Panic(t, func() {
		jet.InitConsumerConfig(&cfg, slip.List{slip.Symbol(":inactive-threshold"), slip.True})
	})
}

func TestConsumerConfigReplicas(t *testing.T) {
	var cfg jetstream.ConsumerConfig
	tt.Panic(t, func() {
		jet.InitConsumerConfig(&cfg, slip.List{slip.Symbol(":replicas"), slip.True})
	})
}

func TestConsumerConfigFilterSubjects(t *testing.T) {
	var cfg jetstream.ConsumerConfig
	tt.Panic(t, func() {
		jet.InitConsumerConfig(&cfg, slip.List{slip.Symbol(":filter-subjects"), slip.True})
	})
}

func TestConsumerConfigMetadata(t *testing.T) {
	var cfg jetstream.ConsumerConfig
	tt.Panic(t, func() {
		jet.InitConsumerConfig(&cfg, slip.List{slip.Symbol(":metadata"), slip.True})
	})
}

func TestConsumerConfigNotKeyword(t *testing.T) {
	var cfg jetstream.ConsumerConfig
	tt.Panic(t, func() {
		jet.InitConsumerConfig(&cfg, slip.List{slip.Symbol(":not-an-option"), slip.True})
	})
}

func TestConsumerConfigPauseUntil(t *testing.T) {
	var cfg jetstream.ConsumerConfig
	tt.Panic(t, func() {
		jet.InitConsumerConfig(&cfg, slip.List{slip.Symbol(":pause-until"), slip.True})
	})
}

func TestConsumerConfigPriorityPolicy(t *testing.T) {
	var cfg jetstream.ConsumerConfig
	jet.InitConsumerConfig(&cfg, slip.List{
		slip.Symbol(":priority-policy"), slip.Symbol(":none"),
	})
	plist := jet.ConsumerConfigPropList(&cfg)
	checkPlistValue(t, ":priority-policy", plist, slip.Symbol(":none"))

	jet.InitConsumerConfig(&cfg, slip.List{
		slip.Symbol(":priority-policy"), slip.Symbol(":overflow"),
	})
	plist = jet.ConsumerConfigPropList(&cfg)
	checkPlistValue(t, ":priority-policy", plist, slip.Symbol(":overflow"))

	tt.Panic(t, func() {
		jet.InitConsumerConfig(&cfg, slip.List{slip.Symbol(":priority-policy"), slip.True})
	})
}

func TestConsumerConfigPinnedTTL(t *testing.T) {
	var cfg jetstream.ConsumerConfig
	tt.Panic(t, func() {
		jet.InitConsumerConfig(&cfg, slip.List{slip.Symbol(":pinned-ttl"), slip.True})
	})
}

func TestConsumerConfigIdleHeartbeat(t *testing.T) {
	var cfg jetstream.ConsumerConfig
	tt.Panic(t, func() {
		jet.InitConsumerConfig(&cfg, slip.List{slip.Symbol(":idle-heartbeat"), slip.True})
	})
}

func TestConsumerConfigPriorityGroups(t *testing.T) {
	var cfg jetstream.ConsumerConfig
	tt.Panic(t, func() {
		jet.InitConsumerConfig(&cfg, slip.List{slip.Symbol(":priority-groups"), slip.True})
	})
}
