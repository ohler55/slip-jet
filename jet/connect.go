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
	// CustomDialer, a CustomDialer not supporter yet
	":custom-reconnect-delay-callback": {
		doc: &slip.DocArg{
			Name: "custom-reconnect-delay-callback",
			Type: "function",
			Text: `Invoked after the library tried every
URL in the server list and failed to reconnect. It passes to the
user the current number of attempts. This function returns the
amount of time the library will sleep before attempting to reconnect
again. It is strongly recommended that this value contains some
jitter to prevent all connections to attempt reconnecting at the same time.`,
		},
		update: func(options *nats.Options, s *slip.Scope, v slip.Object) {
			caller := cl.ResolveToCaller(s, v, 0)
			options.CustomReconnectDelayCB = func(attempts int) (delay time.Duration) {
				result := caller.Call(s, slip.List{slip.Fixnum(attempts)}, 0)
				if num, ok := result.(slip.Real); ok {
					delay = time.Duration(float64(time.Second) * num.RealValue())
				} else {
					slip.PanicType("custom-reconnect-delay", result, "real")
				}
				return
			}
		},
	},
	// Dialer, a *net.Dialer not supporter yet
	":disconnected-callback": {
		doc: &slip.DocArg{
			Name: "disconnected-callback",
			Type: "function",
			Text: `Sets the disconnected handler that is called
whenever the connection is disconnected.
Will not be called if DisconnectedErrCB is set
Deprecated. Use DisconnectedErrCB which passes error that caused
the disconnect event.`,
		},
		update: func(options *nats.Options, s *slip.Scope, v slip.Object) {
			caller := cl.ResolveToCaller(s, v, 0)
			options.DisconnectedCB = func(c *nats.Conn) {
				self := s.Get("self").(*flavors.Instance)
				caller.Call(s, slip.List{self}, 0)
			}
		},
	},
	":disconnected-error-callback": {
		doc: &slip.DocArg{
			Name: "disconnected-error-callback",
			Type: "function",
			Text: `Sets the disconnected error handler that is called
whenever the connection is disconnected.
Disconnected error could be nil, for instance when user explicitly closes the connection.
DisconnectedCB will not be called if DisconnectedErrCB is set.`,
		},
		update: func(options *nats.Options, s *slip.Scope, v slip.Object) {
			caller := cl.ResolveToCaller(s, v, 0)
			options.DisconnectedErrCB = func(c *nats.Conn, err error) {
				self := s.Get("self").(*flavors.Instance)
				caller.Call(s, slip.List{self, slip.NewError("%s", err)}, 0)
			}
		},
	},
	":discovered-servers-callback": {
		doc: &slip.DocArg{
			Name: "discovered-servers-callback",
			Type: "function",
			Text: `Sets the callback that is invoked whenever a new server has joined the cluster.`,
		},
		update: func(options *nats.Options, s *slip.Scope, v slip.Object) {
			caller := cl.ResolveToCaller(s, v, 0)
			options.DiscoveredServersCB = func(c *nats.Conn) {
				self := s.Get("self").(*flavors.Instance)
				caller.Call(s, slip.List{self}, 0)
			}
		},
	},
	":drain-timeout": {
		doc: &slip.DocArg{
			Name: "drain-timeout",
			Type: "real",
			Text: "Sets the timeout for a Drain Operation to complete. Defaults to 30s.",
		},
		update: func(options *nats.Options, s *slip.Scope, v slip.Object) {
			if num, ok := v.(slip.Real); ok {
				options.DrainTimeout = time.Duration(float64(time.Second) * num.RealValue())
			} else {
				slip.PanicType(":drain-timeout", v, "real")
			}
		},
	},
	":flusher-timeout": {
		doc: &slip.DocArg{
			Name: "flusher-timeout",
			Type: "real",
			Text: `Is the maximum time to wait for write operations
to the underlying connection to complete (including the flusher loop).
Defaults to 1m.`,
		},
		update: func(options *nats.Options, s *slip.Scope, v slip.Object) {
			if num, ok := v.(slip.Real); ok {
				options.FlusherTimeout = time.Duration(float64(time.Second) * num.RealValue())
			} else {
				slip.PanicType(":flusher-timeout", v, "real")
			}
		},
	},
	":ignore-auth-error-abort": {
		doc: &slip.DocArg{
			Name: "ignore-auth-error-abort",
			Type: "boolean",
			Text: `If set to true, client opts out of the default connect behavior of aborting
subsequent reconnect attempts if server returns the same auth error twice (regardless of reconnect policy).`,
		},
		update: func(options *nats.Options, s *slip.Scope, v slip.Object) {
			options.IgnoreAuthErrorAbort = (v != nil)
		},
	},
	// InProcessServer, a InProcessConnProvider not supporter yet
	":inbox-prefix": {
		doc: &slip.DocArg{
			Name: "inbox-prefix",
			Type: "string",
			Text: `Sets the default _INBOX prefix.`,
		},
		update: func(options *nats.Options, s *slip.Scope, v slip.Object) {
			if ss, ok := v.(slip.String); ok {
				options.InboxPrefix = string(ss)
			} else {
				slip.PanicType(":inbox-prefix", v, "string")
			}
		},
	},
	":lame-duck-mode-handler": {
		doc: &slip.DocArg{
			Name: "lame-duck-mode-handler",
			Type: "function",
			Text: `Sets the callback to invoke when the server notifies
the connection that it entered lame duck mode, that is, going to
gradually disconnect all its connections before shutting down. This is
often used in deployments when upgrading NATS Servers.`,
		},
		update: func(options *nats.Options, s *slip.Scope, v slip.Object) {
			caller := cl.ResolveToCaller(s, v, 0)
			options.LameDuckModeHandler = func(c *nats.Conn) {
				self := s.Get("self").(*flavors.Instance)
				caller.Call(s, slip.List{self}, 0)
			}
		},
	},
	":max-pings-out": {
		doc: &slip.DocArg{
			Name: "max-pings-out",
			Type: "string",
			Text: `The maximum number of pending ping commands that can
be awaiting a response before raising an ErrStaleConnection error.
Defaults to 2.`,
		},
		update: func(options *nats.Options, s *slip.Scope, v slip.Object) {
			if num, ok := v.(slip.Fixnum); ok {
				options.MaxPingsOut = int(num)
			} else {
				slip.PanicType(":max-pings-out", v, "fixnum")
			}
		},
	},
	":max-reconnect": {
		doc: &slip.DocArg{
			Name: "max-reconnect",
			Type: "string",
			Text: `Sets the number of reconnect attempts that will be
tried before giving up. If negative, then it will never give up
trying to reconnect.
Defaults to 60.`,
		},
		update: func(options *nats.Options, s *slip.Scope, v slip.Object) {
			if num, ok := v.(slip.Fixnum); ok {
				options.MaxReconnect = int(num)
			} else {
				slip.PanicType(":max-reconnect", v, "fixnum")
			}
		},
	},
	":name": {
		doc: &slip.DocArg{
			Name: "name",
			Type: "string",
			Text: `An optional name label which will be sent to the server
on CONNECT to identify the client.`,
		},
		update: func(options *nats.Options, s *slip.Scope, v slip.Object) {
			if ss, ok := v.(slip.String); ok {
				options.Name = string(ss)
			} else {
				slip.PanicType(":name", v, "string")
			}
		},
	},
	":nkey": {
		doc: &slip.DocArg{
			Name: "nkey",
			Type: "string",
			Text: `Sets the public nkey that will be used to authenticate
when connecting to the server. UserJWT and Nkey are mutually exclusive
and if defined, UserJWT will take precedence.`,
		},
		update: func(options *nats.Options, s *slip.Scope, v slip.Object) {
			if ss, ok := v.(slip.String); ok {
				options.Nkey = string(ss)
			} else {
				slip.PanicType(":nkey", v, "string")
			}
		},
	},
	":no-callbacks-after-client-close": {
		doc: &slip.DocArg{
			Name: "no-callbacks-after-client-close",
			Type: "boolean",
			Text: `Allows preventing the invocation of
callbacks after __close__ is called. Client won't receive notifications
when __close__ is invoked by user code. Default is to invoke the callbacks.`,
		},
		update: func(options *nats.Options, s *slip.Scope, v slip.Object) {
			options.NoCallbacksAfterClientClose = (v != nil)
		},
	},
	":no-echo": {
		doc: &slip.DocArg{
			Name: "no-echo",
			Type: "boolean",
			Text: `Configures whether the server will echo back messages
that are sent on this connection if we also have matching subscriptions.
Note this is supported on servers >= version 1.2. Proto 1 or greater.`,
		},
		update: func(options *nats.Options, s *slip.Scope, v slip.Object) {
			options.NoEcho = (v != nil)
		},
	},
	":no-randomize": {
		doc: &slip.DocArg{
			Name: "no-randomize",
			Type: "boolean",
			Text: `Configures whether we will randomize the server pool.`,
		},
		update: func(options *nats.Options, s *slip.Scope, v slip.Object) {
			options.NoRandomize = (v != nil)
		},
	},
	":password": {
		doc: &slip.DocArg{
			Name: "password",
			Type: "string",
			Text: `Sets the password to be used when connecting to a server.`,
		},
		update: func(options *nats.Options, s *slip.Scope, v slip.Object) {
			if ss, ok := v.(slip.String); ok {
				options.Password = string(ss)
			} else {
				slip.PanicType(":password", v, "string")
			}
		},
	},
	":pedantic": {
		doc: &slip.DocArg{
			Name: "pedantic",
			Type: "boolean",
			Text: `Signals the server whether it should be doing further validation of subjects.`,
		},
		update: func(options *nats.Options, s *slip.Scope, v slip.Object) {
			options.Pedantic = (v != nil)
		},
	},
	":ping-interval": {
		doc: &slip.DocArg{
			Name: "ping-interval",
			Type: "real",
			Text: `The period at which the client will be sending ping
commands to the server, disabled if 0 or negative.
Defaults to 2m.`,
		},
		update: func(options *nats.Options, s *slip.Scope, v slip.Object) {
			if num, ok := v.(slip.Real); ok {
				options.PingInterval = time.Duration(float64(time.Second) * num.RealValue())
			} else {
				slip.PanicType(":ping-interval", v, "real")
			}
		},
	},
	":proxy-path": {
		doc: &slip.DocArg{
			Name: "proxy-path",
			Type: "string",
			Text: `For websocket connections, adds a path to connections url.
This is useful when connecting to NATS behind a proxy.`,
		},
		update: func(options *nats.Options, s *slip.Scope, v slip.Object) {
			if ss, ok := v.(slip.String); ok {
				options.ProxyPath = string(ss)
			} else {
				slip.PanicType(":proxy-path", v, "string")
			}
		},
	},
	":reconnect-buf-size": {
		doc: &slip.DocArg{
			Name: "reconnect-buf-size",
			Type: "string",
			Text: `The size of the backing bufio during reconnect.
Once this has been exhausted publish operations will return an error.
Defaults to 8388608 bytes (8MB).`,
		},
		update: func(options *nats.Options, s *slip.Scope, v slip.Object) {
			if num, ok := v.(slip.Fixnum); ok {
				options.ReconnectBufSize = int(num)
			} else {
				slip.PanicType(":reconnect-buf-size", v, "fixnum")
			}
		},
	},
	":reconnect-jitter": {
		doc: &slip.DocArg{
			Name: "reconnect-jitter",
			Type: "real",
			Text: `Sets the upper bound for a random delay added to
ReconnectWait during a reconnect when no TLS is used.
Defaults to 100ms.`,
		},
		update: func(options *nats.Options, s *slip.Scope, v slip.Object) {
			if num, ok := v.(slip.Real); ok {
				options.ReconnectJitter = time.Duration(float64(time.Second) * num.RealValue())
			} else {
				slip.PanicType(":reconnect-jitter", v, "real")
			}
		},
	},
	":reconnect-jitter-tls": {
		doc: &slip.DocArg{
			Name: "reconnect-jitter-tls",
			Type: "real",
			Text: `Sets the upper bound for a random delay added to
ReconnectWait during a reconnect when TLS is used.
Defaults to 1s.`,
		},
		update: func(options *nats.Options, s *slip.Scope, v slip.Object) {
			if num, ok := v.(slip.Real); ok {
				options.ReconnectJitterTLS = time.Duration(float64(time.Second) * num.RealValue())
			} else {
				slip.PanicType(":reconnect-jitter-tls", v, "real")
			}
		},
	},
	":reconnect-wait": {
		doc: &slip.DocArg{
			Name: "reconnect-wait",
			Type: "real",
			Text: `Sets the time to backoff after attempting a reconnect
to a server that we were already connected to previously.
Defaults to 2s.`,
		},
		update: func(options *nats.Options, s *slip.Scope, v slip.Object) {
			if num, ok := v.(slip.Real); ok {
				options.ReconnectWait = time.Duration(float64(time.Second) * num.RealValue())
			} else {
				slip.PanicType(":reconnect-wait", v, "real")
			}
		},
	},
	":reconnected-callback": {
		doc: &slip.DocArg{
			Name: "reconnected-callback",
			Type: "function",
			Text: `Sets the reconnected handler called whenever
the connection is successfully reconnected.`,
		},
		update: func(options *nats.Options, s *slip.Scope, v slip.Object) {
			caller := cl.ResolveToCaller(s, v, 0)
			options.ReconnectedCB = func(c *nats.Conn) {
				self := s.Get("self").(*flavors.Instance)
				caller.Call(s, slip.List{self}, 0)
			}
		},
	},
	// RetryOnFailedConnect bool
	// RootCAsCB RootCAsHandler
	// Secure bool
	// Servers []string
	// SignatureCB SignatureHandler
	// SkipHostLookup bool
	// SubChanLen int
	":timeout": {
		doc: &slip.DocArg{
			Name: "timeout",
			Type: "real",
			Text: "Is the number of seconds to wait before timing out on the connection attempt.",
		},
		update: func(options *nats.Options, s *slip.Scope, v slip.Object) {
			if num, ok := v.(slip.Real); ok {
				options.Timeout = time.Duration(float64(time.Second) * num.RealValue())
			} else {
				slip.PanicType(":timeout", v, "real")
			}
		},
	},
	// TLSCertCB TLSCertHandler
	// TLSConfig *tls.Config
	// TLSHandshakeFirst bool
	// Token string
	// TokenHandler AuthTokenHandler
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
