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

func TestOrderedConfigOk(t *testing.T) {
	var cfg jetstream.OrderedConsumerConfig

	sampleOrderedConfig(&cfg)

	plist := jet.OrderedConfigPropList(&cfg)

	checkPlistValue(t, ":filter-subjects", plist, slip.List{slip.String("test.one"), slip.String("test.two")})
	checkPlistValue(t, ":deliver-policy", plist, slip.Symbol(":last"))
	checkPlistValue(t, ":opt-start-seq", plist, slip.Fixnum(3))
	checkPlistValue(t, ":opt-start-time", plist,
		slip.Time(time.Date(2024, time.December, 9, 19, 18, 17, 16, time.UTC)))
	checkPlistValue(t, ":replay-policy", plist, slip.Symbol(":instant"))
	checkPlistValue(t, ":inactive-threshold", plist, slip.DoubleFloat(4.5))
	checkPlistValue(t, ":headers-only", plist, slip.True)
	checkPlistValue(t, ":max-reset-attempts", plist, slip.Fixnum(2))
}

func sampleOrderedConfig(cfg *jetstream.OrderedConsumerConfig) {
	jet.InitOrderedConfig(cfg, slip.List{
		slip.Symbol(":filter-subjects"), slip.List{slip.String("test.one"), slip.String("test.two")},
		slip.Symbol(":deliver-policy"), slip.Symbol(":last"),
		slip.Symbol(":opt-start-seq"), slip.Fixnum(3),
		slip.Symbol(":opt-start-time"), slip.Time(time.Date(2024, time.December, 9, 19, 18, 17, 16, time.UTC)),
		slip.Symbol(":replay-policy"), slip.Symbol(":instant"),
		slip.Symbol(":inactive-threshold"), slip.DoubleFloat(4.5),
		slip.Symbol(":headers-only"), slip.True,
		slip.Symbol(":max-reset-attempts"), slip.Fixnum(2),
	})
}

func TestOrderedConfigDeliverPolicy(t *testing.T) {
	var cfg jetstream.OrderedConsumerConfig
	tt.Panic(t, func() {
		jet.InitOrderedConfig(&cfg, slip.List{slip.Symbol(":deliver-policy"), slip.True})
	})
}

func TestOrderedConfigOptStartSeq(t *testing.T) {
	var cfg jetstream.OrderedConsumerConfig
	tt.Panic(t, func() {
		jet.InitOrderedConfig(&cfg, slip.List{slip.Symbol(":opt-start-seq"), slip.True})
	})
}

func TestOrderedConfigOptStartTime(t *testing.T) {
	var cfg jetstream.OrderedConsumerConfig
	tt.Panic(t, func() {
		jet.InitOrderedConfig(&cfg, slip.List{slip.Symbol(":opt-start-time"), slip.True})
	})
}

func TestOrderedConfigMaxResetAttempts(t *testing.T) {
	var cfg jetstream.OrderedConsumerConfig
	tt.Panic(t, func() {
		jet.InitOrderedConfig(&cfg, slip.List{slip.Symbol(":max-reset-attempts"), slip.True})
	})
}

func TestOrderedConfigReplayPolicy(t *testing.T) {
	var cfg jetstream.OrderedConsumerConfig
	jet.InitOrderedConfig(&cfg, slip.List{
		slip.Symbol(":replay-policy"), slip.Symbol(":original"),
	})
	plist := jet.OrderedConfigPropList(&cfg)
	checkPlistValue(t, ":replay-policy", plist, slip.Symbol(":original"))

	tt.Panic(t, func() {
		jet.InitOrderedConfig(&cfg, slip.List{slip.Symbol(":replay-policy"), slip.True})
	})
}

func TestOrderedConfigInactiveThreshold(t *testing.T) {
	var cfg jetstream.OrderedConsumerConfig
	tt.Panic(t, func() {
		jet.InitOrderedConfig(&cfg, slip.List{slip.Symbol(":inactive-threshold"), slip.True})
	})
}

func TestOrderedConfigFilterSubjects(t *testing.T) {
	var cfg jetstream.OrderedConsumerConfig
	tt.Panic(t, func() {
		jet.InitOrderedConfig(&cfg, slip.List{slip.Symbol(":filter-subjects"), slip.True})
	})
}

func TestOrderedConfigNotKeyword(t *testing.T) {
	var cfg jetstream.OrderedConsumerConfig
	tt.Panic(t, func() {
		jet.InitOrderedConfig(&cfg, slip.List{slip.Symbol(":not-an-option"), slip.True})
	})
}
