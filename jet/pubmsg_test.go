// Copyright (c) 2024, Peter Ohler, All rights reserved.

package jet_test

import (
	"context"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/ohler55/ojg/pretty"
	"github.com/ohler55/ojg/tt"
	"github.com/ohler55/slip-jet/jet"
)

func TestPubMsgMake(t *testing.T) {
	mi := jet.MakeMsg(nil)

	tt.SameType(t, &jet.PubMsg{}, mi.Any)
}

func TestPubMsgData(t *testing.T) {
	var jm jetstream.Msg = &jet.PubMsg{Body: []byte("data")}
	tt.Equal(t, "data", string(jm.Data()))
}

func TestPubMsgSubject(t *testing.T) {
	var jm jetstream.Msg = &jet.PubMsg{Subj: "sub.ject"}
	tt.Equal(t, "sub.ject", jm.Subject())
}

func TestPubMsgReply(t *testing.T) {
	var jm jetstream.Msg = &jet.PubMsg{Repl: "re.ply"}
	tt.Equal(t, "re.ply", jm.Reply())
}

func TestPubMsgHeaders(t *testing.T) {
	var jm jetstream.Msg = &jet.PubMsg{Head: nats.Header{"head": []string{"str1", "str2"}}}
	tt.Equal(t, "{head: [str1 str2]}", pretty.SEN(jm.Headers()))
}

func TestPubMsgUnpublished(t *testing.T) {
	var jm jetstream.Msg = &jet.PubMsg{}
	tt.NotNil(t, jm.Ack())
	tt.NotNil(t, jm.DoubleAck(context.Background()))
	tt.NotNil(t, jm.Nak())
	tt.NotNil(t, jm.NakWithDelay(time.Second))
	tt.NotNil(t, jm.InProgress())
	tt.NotNil(t, jm.Term())
	tt.NotNil(t, jm.TermWithReason("none"))
}
