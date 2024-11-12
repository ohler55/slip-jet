// Copyright (c) 2024, Peter Ohler, All rights reserved.

package jet

import (
	"sort"
	"strings"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/ohler55/slip"
	"github.com/ohler55/slip/pkg/cl"
	"github.com/ohler55/slip/pkg/flavors"
)

type conOpt struct {
	doc    *slip.DocArg
	update func(options *nats.Options, s *slip.Scope, v slip.Object)
	jopt   func(s *slip.Scope, v slip.Object) jetstream.JetStreamOpt
}

var conOptMap = map[string]*conOpt{
	":allow-reconnect": {
		doc: &slip.DocArg{
			Name: "allow-reconnect",
			Type: "boolean",
			Text: `Enables reconnection logic to be used when we encounter a disconnect from the current server.`,
		},
		update: func(options *nats.Options, s *slip.Scope, v slip.Object) {
			options.AllowReconnect = (v != nil)
		},
	},
	":async-error-callback": {
		doc: &slip.DocArg{
			Name: "async-error-callback",
			Type: "function",
			Text: `Sets the async-error-callback.`,
		},
		update: func(options *nats.Options, s *slip.Scope, v slip.Object) {
			caller := cl.ResolveToCaller(s, v, 0)
			options.AsyncErrorCB = func(c *nats.Conn, sub *nats.Subscription, err error) {
				self := s.Get("self").(*flavors.Instance)
				// TBD add subscription arg
				caller.Call(s, slip.List{self, nil, slip.NewError("%s", err)}, 0)
			}
		},
	},
	":closed-callback": {
		doc: &slip.DocArg{
			Name: "closed-callback",
			Type: "function",
			Text: `Sets the closed-callback.`,
		},
		update: func(options *nats.Options, s *slip.Scope, v slip.Object) {
			caller := cl.ResolveToCaller(s, v, 0)
			options.ClosedCB = func(c *nats.Conn) {
				self := s.Get("self").(*flavors.Instance)
				caller.Call(s, slip.List{self}, 0)
			}
		},
	},
	":compression": {
		doc: &slip.DocArg{
			Name: "compression",
			Type: "boolean",
			Text: `For websocket connections, indicates to the server that the connection
supports compression. If the server does too, then data will be compressed.`,
		},
		update: func(options *nats.Options, s *slip.Scope, v slip.Object) {
			options.Compression = (v != nil)
		},
	},
	":connected-callback": {
		doc: &slip.DocArg{
			Name: "connected-callback",
			Type: "function",
			Text: `Sets the connected-callback.`,
		},
		update: func(options *nats.Options, s *slip.Scope, v slip.Object) {
			caller := cl.ResolveToCaller(s, v, 0)
			options.ConnectedCB = func(c *nats.Conn) {
				self := s.Get("self").(*flavors.Instance)
				caller.Call(s, slip.List{self}, 0)
			}
		},
	},
	":timeout": {
		doc: &slip.DocArg{
			Name: "timeout",
			Type: "real",
			Text: "the number of seconds to wait before timing out on the connection attempt.",
		},
		update: func(options *nats.Options, s *slip.Scope, v slip.Object) {
			if num, ok := v.(slip.Real); ok {
				options.Timeout = time.Duration(float64(time.Second) * num.RealValue())
			} else {
				slip.PanicType(":timeout", v, "real")
			}
		},
	},
	":url": {
		doc: &slip.DocArg{
			Name: "url",
			Type: "string",
			Text: `URL of the jetstream server to connect to.`,
		},
		update: func(options *nats.Options, s *slip.Scope, v slip.Object) {
			if ss, ok := v.(slip.String); ok {
				options.Url = string(ss)
			} else {
				slip.PanicType(":url", v, "string")
			}
		},
	},
	// TBD
	// CustomDialer CustomDialer - maybe not supporter here
	// CustomReconnectDelayCB ReconnectDelayHandler
	// Dialer *net.Dialer
	// DisconnectedCB ConnHandler
	// DisconnectedErrCB ConnErrHandler
	// DiscoveredServersCB ConnHandler
	// DrainTimeout time.Duration
	// FlusherTimeout time.Duration
	// IgnoreAuthErrorAbort bool
	// InProcessServer InProcessConnProvider
	// InboxPrefix string
	// LameDuckModeHandler ConnHandler
	// MaxPingsOut int
	// MaxReconnect int
	// Name string
	// Nkey string
	// NoCallbacksAfterClientClose bool
	// NoEcho bool
	// NoRandomize bool
	// Password string
	// Pedantic bool
	// PingInterval time.Duration
	// ProxyPath string
	// ReconnectBufSize int
	// ReconnectJitter time.Duration
	// ReconnectJitterTLS time.Duration
	// ReconnectWait time.Duration
	// ReconnectedCB ConnHandler
	// RetryOnFailedConnect bool
	// RootCAsCB RootCAsHandler
	// Secure bool
	// Servers []string
	// SignatureCB SignatureHandler
	// SkipHostLookup bool
	// SubChanLen int
	// TLSCertCB TLSCertHandler
	// TLSConfig *tls.Config
	// TLSHandshakeFirst bool
	// Token string
	// TokenHandler AuthTokenHandler
	// UseOldRequestStyle bool
	// User string
	// UserJWT UserJWTHandler
	// Verbose bool

	// TBD js options
	//
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
	var (
		jopts   []jetstream.JetStreamOpt
		prefix  string
		options nats.Options
	)
	for i := 0; i < len(args)-1; i += 2 {
		if sym, ok := args[i].(slip.Symbol); ok {
			key := strings.ToLower(string(sym))
			co := conOptMap[key]
			switch {
			case co.update != nil:
				co.update(&options, s, args[i+1])
			case co.jopt != nil:
				if opt := co.jopt(s, args[i+1]); opt != nil {
					jopts = append(jopts, opt)
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
	if cl.nc, err = options.Connect(); err == nil {
		if 0 < len(prefix) {
			cl.js, err = jetstream.NewWithAPIPrefix(cl.nc, prefix, jopts...)
		} else {
			cl.js, err = jetstream.New(cl.nc, jopts...)
		}
	}
	if err != nil {
		panic(err)
	}
	cl.options = args
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
