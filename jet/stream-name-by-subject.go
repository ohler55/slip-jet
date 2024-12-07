// Copyright (c) 2024, Peter Ohler, All rights reserved.

package jet

import (
	"context"
	"errors"

	"github.com/nats-io/nats.go/jetstream"
	"github.com/ohler55/slip"
	"github.com/ohler55/slip/pkg/flavors"
)

type streamNameBySubjectCaller struct{}

func (caller streamNameBySubjectCaller) Call(s *slip.Scope, args slip.List, _ int) (result slip.Object) {
	self := s.Get("self").(*flavors.Instance)
	flavors.CheckMethodArgCount(self, ":stream-name-by-subject", len(args), 1, 3)
	js := self.Any.(*Client).js

	ctx := context.Background()
	if v, has := slip.GetArgsKeyValue(args[1:], slip.Symbol(":timeout")); has {
		var cf context.CancelFunc
		ctx, cf = context.WithTimeout(ctx, mustBeDuration(v, ":timeout"))
		defer cf()
	}
	if name, err := js.StreamNameBySubject(ctx, slip.MustBeString(args[0], "subject")); err == nil {
		result = slip.String(name)
	} else if !errors.Is(err, jetstream.ErrStreamNotFound) {
		panic(err)
	}
	return
}

func (caller streamNameBySubjectCaller) Docs() string {
	return `__:stream-name-by-subject__ _subject_ &key _timeout_ => _string__
   _subject_ [string] subject to find the the stream name of.
   _:timeout_ [real] the number of seconds to wait before timing out.


Returns a stream name stream listening on given subject. If no
stream is bound to given subject, _nil_ is returned.
`
}
