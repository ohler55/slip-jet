// Copyright (c) 2024, Peter Ohler, All rights reserved.

package jet_test

import (
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"
	"github.com/ohler55/ojg/tt"
	"github.com/ohler55/slip"
	"github.com/ohler55/slip-jet/jet"
	"github.com/ohler55/slip/sliptest"
)

func TestStreamSource(t *testing.T) {
	tm := time.Date(2024, time.December, 3, 19, 33, 22, 123, time.UTC)
	jss := jetstream.StreamSource{
		Name:              "hilltop",
		OptStartSeq:       7,
		OptStartTime:      &tm,
		FilterSubject:     "test.something",
		SubjectTransforms: []jetstream.SubjectTransformConfig{{Source: "test.source", Destination: "test.dest"}},
		External:          &jetstream.ExternalStream{APIPrefix: "pre", DeliverPrefix: "del"},
		Domain:            "domino",
	}
	ss := jet.MakeStreamSource(&jss)
	scope := slip.NewScope()
	scope.Let("ss", ss)
	(&sliptest.Function{
		Scope: scope,
		Source: `(list
                  (send ss :name)
                  (send ss :start-seq)
                  (send ss :start-time)
                  (send ss :filter)
                  (send ss :transforms)
                  (send ss :external-prefix)
                  (send ss :external-deliver-prefix)
                  (send ss :domain))`,
		Expect: `("hilltop" 7 @2024-12-03T19:33:22.000000123Z "test.something"
           (("test.source" . "test.dest")) "pre" "del" "domino")`,
	}).Test(t)

	var jss2 jetstream.StreamSource
	jet.SetJetstreamStreamSource(&jss2, ss)
	tt.Equal(t, jss.Name, jss2.Name)
	tt.Equal(t, jss.OptStartSeq, jss2.OptStartSeq)
	tt.Equal(t, jss.OptStartTime, jss2.OptStartTime)
	tt.Equal(t, jss.FilterSubject, jss2.FilterSubject)
	tt.Equal(t, jss.SubjectTransforms, jss2.SubjectTransforms)
	tt.Equal(t, jss.External, jss2.External)
	tt.Equal(t, jss.Domain, jss2.Domain)
}
