// Copyright (c) 2024, Peter Ohler, All rights reserved.

package jet

import (
	"fmt"
	"strings"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/ohler55/slip"
	"github.com/ohler55/slip/pkg/flavors"
)

func initConnect() {
	slip.Define(
		func(args slip.List) slip.Object {
			f := Connect{Function: slip.Function{Name: "jet-connect", Args: args}}
			f.Self = &f
			return &f
		},
		&slip.FuncDoc{
			Name: "jet-connect",
			Args: []*slip.DocArg{
				{Name: "&key"},
				{
					Name: "url",
					Type: "string",
					Text: `URL of the jetstream server to connect to.`,
				},
				{
					Name: "timeout",
					Type: "real",
					Text: "is the number of seconds to wait before timing out on the connection attempt.",
				},
			},
			Return: "jet-client",
			Text:   `__jet-connect__ attempts to connect to a jet database server with the _url_.`,
			Examples: []string{
				`(jet-connect :url "nats://localhost:4222") => #<jet-client 12345>`,
			},
		}, &Pkg)
}

// Connect represents the make-flow function.
type Connect struct {
	slip.Function
}

// Call the function with the arguments provided.
func (f *Connect) Call(s *slip.Scope, args slip.List, depth int) slip.Object {
	self := clientFlavor.MakeInstance().(*flavors.Instance)
	self.Init(s, args, depth)

	return self
}

var natsOptMap = map[string]func(v slip.Object) nats.Option{
	":timeout": func(v slip.Object) (opt nats.Option) {
		if num, ok := v.(slip.Real); ok {
			opt = nats.Timeout(time.Duration(float64(time.Second) * num.RealValue()))
		} else {
			slip.PanicType(":timeout", v, "real")
		}
		return
	},
}
var jetOptMap = map[string]func(v slip.Object) jetstream.JetStreamOpt{}

type clientInitCaller struct{}

func (caller clientInitCaller) Call(s *slip.Scope, args slip.List, _ int) slip.Object {
	self := s.Get("self").(*flavors.Instance)
	if 0 < len(args) {
		args = args[0].(slip.List)
	}
	nurl := nats.DefaultURL
	var (
		opts  []nats.Option
		jopts []jetstream.JetStreamOpt
	)
	for i := 0; i < len(args)-1; i += 2 {
		if sym, ok := args[i].(slip.Symbol); ok {
			key := strings.ToLower(string(sym))
			if f := natsOptMap[key]; f != nil {
				if opt := f(args[i+1]); opt != nil {
					opts = append(opts, opt)
				}
			} else if f := jetOptMap[key]; f != nil {
				if opt := f(args[i+1]); opt != nil {
					jopts = append(jopts, opt)
				}
			} else if key == ":url" {
				if ss, ok := args[i+1].(slip.String); ok {
					nurl = string(ss)
				} else {
					slip.PanicType(":url", args[i+1], "string")
				}
			}
		} else {
			slip.PanicType("key", args[i], "keyword")
		}
	}
	// TBD set nats options
	// walk args
	//  if key is in natsOptMap then set opts
	//  if key is in jetOptMap then set jopts
	// value in maps is function to call with value from args[i+1]

	// TBD set js options
	//  trace
	// pub error handler
	// pub max pending

	nc, err := nats.Connect(nurl, opts...)
	if err != nil {
		panic(err)
	}

	var js jetstream.JetStream

	// TBD if prefix provided use that

	if js, err = jetstream.New(nc, jopts...); err != nil {
		panic(err)
	}
	self.Any = js

	fmt.Printf("*** init args: %s\n", args)
	// TBD
	// jetstream.New()
	//  with domain or api prefix
	//  with client trace
	//  with async error handler
	//  with async max pending

	return nil
}

// TBD build docs from connect args
func (caller clientInitCaller) Docs() string {
	return `__:init__ &key


Sets the initial value when _make-instance_ is called.
`
}
