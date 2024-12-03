// Copyright (c) 2024, Peter Ohler, All rights reserved.

package jet_test

import (
	"testing"

	"github.com/nats-io/nats.go/jetstream"
	"github.com/ohler55/ojg/tt"
	"github.com/ohler55/slip"
	"github.com/ohler55/slip-jet/jet"
)

func TestStreamConfigOk(t *testing.T) {
	var cfg jetstream.StreamConfig

	jet.InitStreamConfig(&cfg, slip.List{
		slip.Symbol(":name"), slip.String("river"),
		slip.Symbol(":allow-direct"), slip.True,
		slip.Symbol(":allow-rollup"), slip.True,
		slip.Symbol(":compression"), slip.True,
		slip.Symbol(":consumer-limits"), slip.List{
			slip.DoubleFloat(2.5),
			slip.Fixnum(7),
		},
		slip.Symbol(":deny-delete"), slip.True,
		slip.Symbol(":deny-purge"), slip.True,
		slip.Symbol(":description"), slip.String("description"),
		slip.Symbol(":discard"), slip.Symbol(":new"),
		slip.Symbol(":discard-new-per-subject"), slip.True,
		slip.Symbol(":duplicates"), slip.DoubleFloat(1.5),
		slip.Symbol(":first-seq"), slip.Fixnum(3),
		slip.Symbol(":max-age"), slip.Fixnum(99),
		slip.Symbol(":max-bytes"), slip.Fixnum(4096),
		slip.Symbol(":max-consumers"), slip.Fixnum(9),
		slip.Symbol(":max-msg-size"), slip.Fixnum(1024),
		slip.Symbol(":max-msgs"), slip.Fixnum(5),
		slip.Symbol(":max-msgs-per-subject"), slip.Fixnum(2),
		slip.Symbol(":metadata"), slip.List{slip.String("meta"), slip.String("data")},
		slip.Symbol(":mirror"), jet.MakeStreamSource(&jetstream.StreamSource{Name: "source"}),
		slip.Symbol(":mirror-direct"), slip.True,
		slip.Symbol(":no-ack"), slip.True,
		slip.Symbol(":placement"), slip.List{slip.String("cluster"), slip.String("tag")},
		slip.Symbol(":re-publish"), slip.List{slip.String("source"), slip.String("destination"), slip.True},
		slip.Symbol(":replicas"), slip.Fixnum(2),
		slip.Symbol(":retention"), slip.Symbol(":limit"),
		slip.Symbol(":sealed"), slip.True,
		slip.Symbol(":sources"), slip.List{jet.MakeStreamSource(&jetstream.StreamSource{Name: "source2"})},
		slip.Symbol(":storage"), slip.Symbol(":memory"),
		slip.Symbol(":subject-transform"), slip.List{slip.String("src"), slip.String("dest")},
		slip.Symbol(":subjects"), slip.List{slip.String("test.one"), slip.String("test.two")},
	})

	plist := jet.StreamConfigPropList(&cfg)

	checkPlistValue(t, ":name", plist, slip.String("river"))
	checkPlistValue(t, ":allow-direct", plist, slip.True)
	checkPlistValue(t, ":allow-rollup", plist, slip.True)
	checkPlistValue(t, ":compression", plist, slip.True)
	checkPlistValue(t, ":consumer-limits", plist, slip.List{slip.DoubleFloat(2.5), slip.Fixnum(7)})
	checkPlistValue(t, ":deny-delete", plist, slip.True)
	checkPlistValue(t, ":deny-purge", plist, slip.True)
	checkPlistValue(t, ":description", plist, slip.String("description"))
	checkPlistValue(t, ":discard", plist, slip.Symbol(":new"))
	checkPlistValue(t, ":discard-new-per-subject", plist, slip.True)
	checkPlistValue(t, ":duplicates", plist, slip.DoubleFloat(1.5))
	checkPlistValue(t, ":first-seq", plist, slip.Fixnum(3))
	checkPlistValue(t, ":max-age", plist, slip.DoubleFloat(99))
	checkPlistValue(t, ":max-bytes", plist, slip.Fixnum(4096))
	checkPlistValue(t, ":max-consumers", plist, slip.Fixnum(9))
	checkPlistValue(t, ":max-msg-size", plist, slip.Fixnum(1024))
	checkPlistValue(t, ":max-msgs", plist, slip.Fixnum(5))
	checkPlistValue(t, ":max-msgs-per-subject", plist, slip.Fixnum(2))
	checkPlistValue(t, ":metadata", plist, slip.List{slip.String("meta"), slip.String("data")})
	checkPlistValue(t, ":mirror", plist, jet.MakeStreamSource(&jetstream.StreamSource{Name: "source"}))
	checkPlistValue(t, ":mirror-direct", plist, slip.True)
	checkPlistValue(t, ":no-ack", plist, slip.True)
	checkPlistValue(t, ":placement", plist, slip.List{slip.String("cluster"), slip.String("tag")})
	checkPlistValue(t, ":re-publish", plist, slip.List{slip.String("source"), slip.String("destination"), slip.True})
	checkPlistValue(t, ":replicas", plist, slip.Fixnum(2))
	checkPlistValue(t, ":retention", plist, slip.Symbol(":limit"))
	checkPlistValue(t, ":sealed", plist, slip.True)
	checkPlistValue(t, ":sources", plist, slip.List{jet.MakeStreamSource(&jetstream.StreamSource{Name: "source2"})})
	checkPlistValue(t, ":storage", plist, slip.Symbol(":memory"))
	checkPlistValue(t, ":subject-transform", plist, slip.List{slip.String("src"), slip.String("dest")})
	checkPlistValue(t, ":subjects", plist, slip.List{slip.String("test.one"), slip.String("test.two")})
}

func TestStreamConfigBoolNil(t *testing.T) {
	var cfg jetstream.StreamConfig
	jet.InitStreamConfig(&cfg, slip.List{
		slip.Symbol(":compression"), nil,
	})
	plist := jet.StreamConfigPropList(&cfg)
	checkPlistValue(t, ":compression", plist, nil)
}

func TestStreamConfigConsumerLimits(t *testing.T) {
	var cfg jetstream.StreamConfig
	tt.Panic(t, func() {
		jet.InitStreamConfig(&cfg, slip.List{slip.Symbol(":consumer-limits"), slip.List{slip.True, slip.Fixnum(7)}})
	})
	tt.Panic(t, func() {
		jet.InitStreamConfig(&cfg, slip.List{slip.Symbol(":consumer-limits"), slip.List{slip.Fixnum(7), slip.True}})
	})
	tt.Panic(t, func() {
		jet.InitStreamConfig(&cfg, slip.List{slip.Symbol(":consumer-limits"), slip.True})
	})
}

func TestStreamConfigDiscard(t *testing.T) {
	var cfg jetstream.StreamConfig
	jet.InitStreamConfig(&cfg, slip.List{
		slip.Symbol(":discard"), slip.Symbol(":old"),
	})
	plist := jet.StreamConfigPropList(&cfg)
	checkPlistValue(t, ":discard", plist, slip.Symbol(":old"))

	tt.Panic(t, func() {
		jet.InitStreamConfig(&cfg, slip.List{slip.Symbol(":discard"), slip.Symbol(":quux")})
	})
}

func TestStreamConfigDuplicates(t *testing.T) {
	var cfg jetstream.StreamConfig
	tt.Panic(t, func() {
		jet.InitStreamConfig(&cfg, slip.List{slip.Symbol(":duplicates"), slip.True})
	})
}

func TestStreamConfigFirstSeq(t *testing.T) {
	var cfg jetstream.StreamConfig
	tt.Panic(t, func() {
		jet.InitStreamConfig(&cfg, slip.List{slip.Symbol(":first-seq"), slip.True})
	})
}

func TestStreamConfigMaxAge(t *testing.T) {
	var cfg jetstream.StreamConfig
	tt.Panic(t, func() {
		jet.InitStreamConfig(&cfg, slip.List{slip.Symbol(":max-age"), slip.True})
	})
}

func TestStreamConfigMaxBytes(t *testing.T) {
	var cfg jetstream.StreamConfig
	tt.Panic(t, func() {
		jet.InitStreamConfig(&cfg, slip.List{slip.Symbol(":max-bytes"), slip.True})
	})
}

func TestStreamConfigMaxConsumers(t *testing.T) {
	var cfg jetstream.StreamConfig
	tt.Panic(t, func() {
		jet.InitStreamConfig(&cfg, slip.List{slip.Symbol(":max-consumers"), slip.True})
	})
}

func TestStreamConfigMaxMsgSize(t *testing.T) {
	var cfg jetstream.StreamConfig
	tt.Panic(t, func() {
		jet.InitStreamConfig(&cfg, slip.List{slip.Symbol(":max-msg-size"), slip.True})
	})
}

func TestStreamConfigMaxMsgs(t *testing.T) {
	var cfg jetstream.StreamConfig
	tt.Panic(t, func() {
		jet.InitStreamConfig(&cfg, slip.List{slip.Symbol(":max-msgs"), slip.True})
	})
}

func TestStreamConfigMaxMsgsPerSubject(t *testing.T) {
	var cfg jetstream.StreamConfig
	tt.Panic(t, func() {
		jet.InitStreamConfig(&cfg, slip.List{slip.Symbol(":max-msgs-per-subject"), slip.True})
	})
}

func TestStreamConfigMetadata(t *testing.T) {
	var cfg jetstream.StreamConfig
	tt.Panic(t, func() {
		jet.InitStreamConfig(&cfg, slip.List{slip.Symbol(":metadata"), slip.True})
	})
}

func TestStreamConfigMirror(t *testing.T) {
	var cfg jetstream.StreamConfig
	tt.Panic(t, func() {
		jet.InitStreamConfig(&cfg, slip.List{slip.Symbol(":mirror"), slip.True})
	})
}

func TestStreamConfigPlacement(t *testing.T) {
	var cfg jetstream.StreamConfig
	tt.Panic(t, func() {
		jet.InitStreamConfig(&cfg, slip.List{slip.Symbol(":placement"), slip.True})
	})
}

func TestStreamConfigRePublish(t *testing.T) {
	var cfg jetstream.StreamConfig
	tt.Panic(t, func() {
		jet.InitStreamConfig(&cfg, slip.List{slip.Symbol(":re-publish"), slip.True})
	})
}

func TestStreamConfigReplicas(t *testing.T) {
	var cfg jetstream.StreamConfig
	tt.Panic(t, func() {
		jet.InitStreamConfig(&cfg, slip.List{slip.Symbol(":replicas"), slip.True})
	})
}

func TestStreamConfigRetention(t *testing.T) {
	var cfg jetstream.StreamConfig
	jet.InitStreamConfig(&cfg, slip.List{
		slip.Symbol(":retention"), slip.Symbol(":interest"),
	})
	plist := jet.StreamConfigPropList(&cfg)
	checkPlistValue(t, ":retention", plist, slip.Symbol(":interest"))

	jet.InitStreamConfig(&cfg, slip.List{
		slip.Symbol(":retention"), slip.Symbol(":queue"),
	})
	plist = jet.StreamConfigPropList(&cfg)
	checkPlistValue(t, ":retention", plist, slip.Symbol(":queue"))

	tt.Panic(t, func() {
		jet.InitStreamConfig(&cfg, slip.List{slip.Symbol(":retention"), slip.True})
	})
}

func TestStreamConfigSources(t *testing.T) {
	var cfg jetstream.StreamConfig
	tt.Panic(t, func() {
		jet.InitStreamConfig(&cfg, slip.List{slip.Symbol(":sources"), slip.True})
	})
	tt.Panic(t, func() {
		jet.InitStreamConfig(&cfg, slip.List{slip.Symbol(":sources"), slip.List{slip.True}})
	})
}

func TestStreamConfigStorage(t *testing.T) {
	var cfg jetstream.StreamConfig
	jet.InitStreamConfig(&cfg, slip.List{
		slip.Symbol(":storage"), slip.Symbol(":file"),
	})
	plist := jet.StreamConfigPropList(&cfg)
	checkPlistValue(t, ":storage", plist, slip.Symbol(":file"))

	tt.Panic(t, func() {
		jet.InitStreamConfig(&cfg, slip.List{slip.Symbol(":storage"), slip.Symbol(":quux")})
	})
}

func TestStreamConfigSubjectTransform(t *testing.T) {
	var cfg jetstream.StreamConfig
	tt.Panic(t, func() {
		jet.InitStreamConfig(&cfg, slip.List{slip.Symbol(":subject-transform"), slip.True})
	})
}

func TestStreamConfigSubjects(t *testing.T) {
	var cfg jetstream.StreamConfig
	tt.Panic(t, func() {
		jet.InitStreamConfig(&cfg, slip.List{slip.Symbol(":subjects"), slip.True})
	})
}

func TestStreamConfigBadKeyword(t *testing.T) {
	var cfg jetstream.StreamConfig
	tt.Panic(t, func() { jet.InitStreamConfig(&cfg, slip.List{slip.Symbol(":nonsense"), slip.True}) })
}

func checkPlistValue(t *testing.T, key string, plist slip.List, expect slip.Object) {
	v, _ := slip.GetArgsKeyValue(plist, slip.Symbol(key))
	tt.Equal(t, expect, v)
}
