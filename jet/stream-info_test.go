// Copyright (c) 2024, Peter Ohler, All rights reserved.

package jet_test

import (
	"fmt"
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"
	"github.com/ohler55/slip"
	"github.com/ohler55/slip-jet/jet"
	"github.com/ohler55/slip/sliptest"
)

func TestStreamInfoCached(t *testing.T) {
	var stream mockStream
	sampleStreamConfig(&stream.info.Config)
	sampleStreamState(&stream.info.State)

	scope := slip.NewScope()
	scope.Let(slip.Symbol("js"), jet.MakeStream(&stream))
	(&sliptest.Function{
		Scope: scope,
		Source: `(let ((info (send js :info :cached t)))
                  (send (send info :state) :bytes))`,
		Expect: "6000",
	}).Test(t)
}

func TestStreamInfoServer(t *testing.T) {
	var stream mockStream
	sampleStreamConfig(&stream.info.Config)
	sampleStreamState(&stream.info.State)

	scope := slip.NewScope()
	scope.Let(slip.Symbol("js"), jet.MakeStream(&stream))
	(&sliptest.Function{
		Scope: scope,
		Source: `(let ((info (send js :info :timeout 0.1 :deleted t)))
                  (send (send info :state) :bytes))`,
		Expect: "6000",
	}).Test(t)
	stream.err = fmt.Errorf("dummy error")
	(&sliptest.Function{
		Scope:     scope,
		Source:    `(send js :info :timeout 0.1)`,
		PanicType: slip.ErrorSymbol,
	}).Test(t)
}

func TestStreamInfoState(t *testing.T) {
	var info jetstream.StreamInfo
	sampleStreamState(&info.State)
	scope := slip.NewScope()
	scope.Let(slip.Symbol("ii"), jet.MakeStreamInfo(&info))
	(&sliptest.Function{
		Scope:  scope,
		Source: `(let ((state (send ii :state))) (list (send state :msgs) (send state :bytes)))`,
		Expect: "(100 6000)",
	}).Test(t)
}

func TestStreamInfoCreated(t *testing.T) {
	info := jetstream.StreamInfo{
		Created: time.Date(2024, time.December, 7, 19, 00, 2, 123, time.UTC),
	}
	sampleStreamState(&info.State)
	scope := slip.NewScope()
	scope.Let(slip.Symbol("ii"), jet.MakeStreamInfo(&info))
	(&sliptest.Function{
		Scope:  scope,
		Source: `(send ii :created)`,
		Expect: "@2024-12-07T19:00:02.000000123Z",
	}).Test(t)
}

func TestStreamInfoCluster(t *testing.T) {
	info := jetstream.StreamInfo{
		Cluster: &jetstream.ClusterInfo{
			Name:     "cluster",
			Leader:   "ichiban",
			Replicas: []*jetstream.PeerInfo{{Name: "pier", Current: true, Offline: true, Active: time.Minute, Lag: 3}},
		},
	}
	sampleStreamState(&info.State)
	scope := slip.NewScope()
	scope.Let(slip.Symbol("ii"), jet.MakeStreamInfo(&info))
	(&sliptest.Function{
		Scope:  scope,
		Source: `(send ii :cluster)`,
		Expect: `(:name "cluster" :leader "ichiban" :replicas
       ((:name "pier" :current t :offline t :active 60 :lag 3)))`,
	}).Test(t)
}

func TestStreamInfoMirror(t *testing.T) {
	info := jetstream.StreamInfo{
		Mirror: &jetstream.StreamSourceInfo{
			Name:              "mirror",
			Lag:               7,
			Active:            -1,
			FilterSubject:     "test.>",
			SubjectTransforms: []jetstream.SubjectTransformConfig{{Source: "src", Destination: "dest"}},
		},
	}
	sampleStreamState(&info.State)
	scope := slip.NewScope()
	scope.Let(slip.Symbol("ii"), jet.MakeStreamInfo(&info))
	(&sliptest.Function{
		Scope:  scope,
		Source: `(send ii :mirror)`,
		Expect: "/#<jet-stream-source-info [0-9a-f]+>/",
	}).Test(t)
}

func TestStreamInfoSources(t *testing.T) {
	info := jetstream.StreamInfo{
		Sources: []*jetstream.StreamSourceInfo{
			{
				Name:              "mirror",
				Lag:               7,
				Active:            time.Minute,
				FilterSubject:     "test.>",
				SubjectTransforms: []jetstream.SubjectTransformConfig{{Source: "src", Destination: "dest"}},
			},
		},
	}
	sampleStreamState(&info.State)
	scope := slip.NewScope()
	scope.Let(slip.Symbol("ii"), jet.MakeStreamInfo(&info))
	(&sliptest.Function{
		Scope:  scope,
		Source: `(send ii :sources)`,
		Expect: `/\(#<jet-stream-source-info [0-9a-f]+>\)/`,
	}).Test(t)
}

func TestStreamInfoTimestamp(t *testing.T) {
	info := jetstream.StreamInfo{
		TimeStamp: time.Date(2024, time.December, 7, 19, 00, 2, 123, time.UTC),
	}
	sampleStreamState(&info.State)
	scope := slip.NewScope()
	scope.Let(slip.Symbol("ii"), jet.MakeStreamInfo(&info))
	(&sliptest.Function{
		Scope:  scope,
		Source: `(send ii :timestamp)`,
		Expect: "@2024-12-07T19:00:02.000000123Z",
	}).Test(t)
}

func TestStreamInfoGoMakeOnly(t *testing.T) {
	(&sliptest.Function{
		Source:    `(make-instance 'jet-stream-info)`,
		PanicType: slip.ErrorSymbol,
	}).Test(t)
}
