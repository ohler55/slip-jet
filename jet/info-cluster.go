// Copyright (c) 2024, Peter Ohler, All rights reserved.

package jet

import (
	"time"

	"github.com/nats-io/nats.go/jetstream"
	"github.com/ohler55/slip"
	"github.com/ohler55/slip/pkg/flavors"
)

type infoClusterCaller struct{}

func (caller infoClusterCaller) Call(s *slip.Scope, args slip.List, _ int) slip.Object {
	self := s.Get("self").(*flavors.Instance)
	cluster := self.Any.(jetstream.StreamInfo).Cluster
	var reps slip.List
	for _, pi := range cluster.Replicas {
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
		reps = append(reps, slip.List{
			slip.Symbol(":name"), slip.String(pi.Name),
			slip.Symbol(":current"), current,
			slip.Symbol(":offline"), offline,
			slip.Symbol(":active"), slip.DoubleFloat(float64(pi.Active) / float64(time.Second)),
			slip.Symbol(":lag"), slip.Fixnum(pi.Lag),
		})
	}
	return slip.List{
		slip.Symbol(":name"), slip.String(cluster.Name),
		slip.Symbol(":leader"), slip.String(cluster.Leader),
		slip.Symbol(":replicas"), reps,
	}
}

func (caller infoClusterCaller) Docs() string {
	return `__:cluster__ => _property list_


Returns the information about the cluster to which this stream belongs (if
applicable). The properties are _:name_, _:leader_, and _:replicas_. The
_:replicas_ property is a property list of _:name_, _:current_, _:offline_,
_:active_, and _:lag_.
`
}
