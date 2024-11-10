// Copyright (c) 2024, Peter Ohler, All rights reserved.

package jet

import (
	"sort"
	"strings"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/ohler55/slip"
	"github.com/ohler55/slip/pkg/flavors"
)

// TBD maybe one map for args, docs, and eval
type conOpt struct {
	doc  *slip.DocArg
	opt  func(v slip.Object) nats.Option
	jopt func(v slip.Object) jetstream.JetStreamOpt
}

var conOptMap = map[string]*conOpt{
	":client-cert": {
		doc: &slip.DocArg{
			Name: "client-cert",
			Type: "list",
			Text: `a helper option to provide the client certificate from a file.
If Secure is not already set this will set it as well. The list must be a list of cert filename followed
by the key filename.`,
		},
		opt: func(v slip.Object) (opt nats.Option) {
			if list, ok := v.(slip.List); ok && len(list) == 2 {
				cert, _ := list[0].(slip.String)
				key, _ := list[1].(slip.String)
				if 0 < len(cert) && 0 < len(key) {
					opt = nats.ClientCert(string(cert), string(key))
				}
			}
			if opt == nil {
				slip.PanicType(":client-cert", v, "list of cert and key filenames")
			}
			return
		},
	},
	// TBD others
	":timeout": {
		doc: &slip.DocArg{
			Name: "timeout",
			Type: "real",
			Text: "the number of seconds to wait before timing out on the connection attempt.",
		},
		opt: func(v slip.Object) (opt nats.Option) {
			if num, ok := v.(slip.Real); ok {
				opt = nats.Timeout(time.Duration(float64(time.Second) * num.RealValue()))
			} else {
				slip.PanicType(":timeout", v, "real")
			}
			return
		},
	},
	":url": {
		doc: &slip.DocArg{
			Name: "url",
			Type: "string",
			Text: `URL of the jetstream server to connect to.`,
		},
	},
}

func initConnect() {
	args := make([]*slip.DocArg, len(conOptMap)+1)
	keys := make([]string, 0, len(conOptMap))
	for k := range conOptMap {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	args[0] = &slip.DocArg{Name: "&key"}
	for i, k := range keys {
		args[i+1] = conOptMap[k].doc
	}
	slip.Define(
		func(args slip.List) slip.Object {
			f := Connect{Function: slip.Function{Name: "jet-connect", Args: args}}
			f.Self = &f
			return &f
		},
		&slip.FuncDoc{
			Name:   "jet-connect",
			Args:   args,
			Return: "jet-client",
			Text:   `__jet-connect__ attempts to connect to a jetstream server with the _url_.`,
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

type clientInitCaller struct{}

func (caller clientInitCaller) Call(s *slip.Scope, args slip.List, _ int) slip.Object {
	self := s.Get("self").(*flavors.Instance)
	if 0 < len(args) {
		args = args[0].(slip.List)
	}
	nurl := nats.DefaultURL
	var (
		opts   []nats.Option
		jopts  []jetstream.JetStreamOpt
		prefix string
	)
	for i := 0; i < len(args)-1; i += 2 {
		if sym, ok := args[i].(slip.Symbol); ok {
			key := strings.ToLower(string(sym))
			co := conOptMap[key]
			switch {
			case co.opt != nil:
				if opt := co.opt(args[i+1]); opt != nil {
					opts = append(opts, opt)
				}
			case co.jopt != nil:
				if opt := co.jopt(args[i+1]); opt != nil {
					jopts = append(jopts, opt)
				}
			case key == ":url":
				if ss, ok := args[i+1].(slip.String); ok {
					nurl = string(ss)
				} else {
					slip.PanicType(":url", args[i+1], "string")
				}
			case key == ":prefix":
				if ss, ok := args[i+1].(slip.String); ok {
					prefix = string(ss)
				} else {
					slip.PanicType(":prefix", args[i+1], "string")
				}
			}
		} else {
			// TBD might not need this if check is done by make-instance
			slip.PanicType("key", args[i], "keyword")
		}
	}
	var (
		cl  client
		err error
	)
	if cl.nc, err = nats.Connect(nurl, opts...); err == nil {
		if 0 < len(prefix) {
			cl.js, err = jetstream.NewWithAPIPrefix(cl.nc, prefix, jopts...)
		} else {
			cl.js, err = jetstream.New(cl.nc, jopts...)
		}
	}
	if err != nil {
		panic(err)
	}
	self.Any = &cl

	return nil
}

func (caller clientInitCaller) Docs() string {
	var doc []byte

	doc = append(doc, "__:init__ &key"...)
	keys := make([]string, 0, len(conOptMap))
	for k := range conOptMap {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		co := conOptMap[k]
		doc = append(doc, ' ', '_')
		doc = append(doc, co.doc.Name...)
		doc = append(doc, '_')
	}
	for _, k := range keys {
		co := conOptMap[k]
		doc = append(doc, "\n   _"...)
		doc = append(doc, k...)
		doc = append(doc, '_', ' ', '[')
		doc = append(doc, co.doc.Type...)
		doc = append(doc, ']', ' ')
		doc = append(doc, co.doc.Text...)
	}
	return string(append(doc, "\n\n\nSets the initial value when _make-instance_ is called.\n"...))
}
