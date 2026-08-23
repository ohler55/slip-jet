// Copyright (c) 2024, Peter Ohler, All rights reserved.

package jet

import (
	"github.com/ohler55/slip"
	"github.com/ohler55/slip/pkg/flavors"
)

type optionsCaller struct{}

func (caller optionsCaller) Call(s *slip.Scope, args slip.List, depth int) slip.Object {
	self := s.Get("self").(*flavors.Instance)
	slip.MethodArgCountCheck(s, depth, self, ":options", len(args), 0, 0)
	cl := self.Any.(*Client)
	// Use both the nats.Conn options as well as the saved options to
	// lookup functions.
	options := make(slip.List, 0, 100) // up to 50 pairs expected
	options = caller.appendBool(options, ":allow-reconnect", cl.nc.Opts.AllowReconnect)
	options = caller.appendFunc(options, ":async-error-callback", cl.nc.Opts.AsyncErrorCB, cl.options)
	options = caller.appendFunc(options, ":closed-callback", cl.nc.Opts.ClosedCB, cl.options)
	options = caller.appendBool(options, ":compression", cl.nc.Opts.Compression)
	options = caller.appendFunc(options, ":connected-callback", cl.nc.Opts.ConnectedCB, cl.options)
	// CustomDialer, a CustomDialer not supported yet
	options = caller.appendFunc(options, ":custom-reconnect-delay-callback",
		cl.nc.Opts.CustomReconnectDelayCB, cl.options)
	// Dialer, a *net.Dialer not supported yet
	options = caller.appendFunc(options, ":disconnected-callback", cl.nc.Opts.DisconnectedCB, cl.options)
	options = caller.appendFunc(options, ":disconnected-error-callback", cl.nc.Opts.DisconnectedErrCB, cl.options)
	options = caller.appendFunc(options, ":discovered-servers-callback", cl.nc.Opts.DiscoveredServersCB, cl.options)
	options = append(options, slip.Symbol(":drain-timeout"), slip.DoubleFloat(cl.nc.Opts.DrainTimeout))
	options = append(options, slip.Symbol(":flusher-timeout"), slip.DoubleFloat(cl.nc.Opts.FlusherTimeout))
	options = caller.appendBool(options, ":ignore-auth-error-abort", cl.nc.Opts.IgnoreAuthErrorAbort)
	options = caller.appendBool(options, ":ignore-discovered-servers", cl.nc.Opts.IgnoreDiscoveredServers)
	// InProcessServer, a InProcessConnProvider not supported yet
	options = caller.appendString(options, ":inbox-prefix", cl.nc.Opts.InboxPrefix)
	options = caller.appendFunc(options, ":lame-duck-mode-handler", cl.nc.Opts.LameDuckModeHandler, cl.options)
	options = append(options, slip.Symbol(":max-pings-out"), slip.Fixnum(cl.nc.Opts.MaxPingsOut))
	options = append(options, slip.Symbol(":max-reconnect"), slip.Fixnum(cl.nc.Opts.MaxReconnect))
	options = caller.appendString(options, ":name", cl.nc.Opts.Name)
	options = caller.appendString(options, ":nkey", cl.nc.Opts.Nkey)
	options = caller.appendBool(options, ":no-callbacks-after-client-close", cl.nc.Opts.NoCallbacksAfterClientClose)
	options = caller.appendBool(options, ":no-echo", cl.nc.Opts.NoEcho)
	options = caller.appendBool(options, ":no-randomize", cl.nc.Opts.NoRandomize)
	options = caller.appendString(options, ":password", cl.nc.Opts.Password)
	options = caller.appendBool(options, ":pedantic", cl.nc.Opts.Pedantic)
	options = caller.appendBool(options, ":permission-err-on-subscribe", cl.nc.Opts.PermissionErrOnSubscribe)
	options = append(options, slip.Symbol(":ping-interval"), slip.DoubleFloat(cl.nc.Opts.PingInterval))
	options = caller.appendString(options, ":proxy-path", cl.nc.Opts.ProxyPath)
	options = append(options, slip.Symbol(":reconnect-buf-size"), slip.Fixnum(cl.nc.Opts.ReconnectBufSize))
	options = append(options, slip.Symbol(":reconnect-jitter"), slip.DoubleFloat(cl.nc.Opts.ReconnectJitter))
	options = append(options, slip.Symbol(":reconnect-jitter-tls"), slip.DoubleFloat(cl.nc.Opts.ReconnectJitterTLS))
	options = append(options, slip.Symbol(":reconnect-wait"), slip.DoubleFloat(cl.nc.Opts.ReconnectWait))
	options = caller.appendFunc(options, ":reconnected-callback", cl.nc.Opts.ReconnectedCB, cl.options)
	options = caller.appendBool(options, ":retry-on-failed-connect", cl.nc.Opts.RetryOnFailedConnect)
	// RootCAsCB, a RootCAsHandler not supported yet
	options = caller.appendBool(options, ":secure", cl.nc.Opts.Secure)
	options = caller.appendStringList(options, ":servers", cl.nc.Opts.Servers)
	options = caller.appendFunc(options, ":signature-callback", cl.nc.Opts.SignatureCB, cl.options)
	options = caller.appendBool(options, ":skip-host-lookup", cl.nc.Opts.SkipHostLookup)
	options = append(options, slip.Symbol(":sub-chan-len"), slip.Fixnum(cl.nc.Opts.SubChanLen))
	options = append(options, slip.Symbol(":timeout"), slip.DoubleFloat(cl.nc.Opts.Timeout))
	// TLSCertCB, a TLSCertHandler not supported yet
	// TLSConfig, a *tls.Config not supported yet
	options = caller.appendBool(options, ":tls-handshake-first", cl.nc.Opts.TLSHandshakeFirst)
	options = caller.appendString(options, ":token", cl.nc.Opts.Token)
	options = caller.appendFunc(options, ":token-handler", cl.nc.Opts.TokenHandler, cl.options)
	options = caller.appendString(options, ":url", cl.nc.Opts.Url)
	options = caller.appendBool(options, ":use-old-request-style", cl.nc.Opts.UseOldRequestStyle)
	options = caller.appendString(options, ":user", cl.nc.Opts.User)
	options = caller.appendFunc(options, ":user-jwt", cl.nc.Opts.UserJWT, cl.options)
	options = caller.appendBool(options, ":verbose", cl.nc.Opts.Verbose)
	options = append(options, slip.Symbol(":write-buffer-size"), slip.Fixnum(cl.nc.Opts.WriteBufferSize))

	options = caller.appendFromArgs(options, ":user-credentials", cl.options)
	options = caller.appendFromArgs(options, ":prefix", cl.options)
	options = caller.appendFromArgs(options, ":publish-async-error-handler", cl.options)
	options = caller.appendFromArgs(options, ":publish-async-max-pending", cl.options)

	return options
}

func (caller optionsCaller) FuncDocs() *slip.FuncDoc {
	return &slip.FuncDoc{
		Name:   ":options",
		Text:   `Returns the options used for the client connection as a property list.`,
		Return: "property-list",
	}
}

func (caller optionsCaller) appendBool(options slip.List, name string, value bool) slip.List {
	var pv slip.Object
	if value {
		pv = slip.True
	}
	return append(options, slip.Symbol(name), pv)
}

func (caller optionsCaller) appendFunc(options slip.List, name string, value any, args slip.List) slip.List {
	var pv slip.Object
	key := slip.Symbol(name)
	if value != nil {
		pv, _ = slip.GetArgsKeyValue(args, key)
	}
	return append(options, key, pv)
}

func (caller optionsCaller) appendString(options slip.List, name string, value string) slip.List {
	var pv slip.Object
	if 0 < len(value) {
		pv = slip.String(value)
	}
	return append(options, slip.Symbol(name), pv)
}

func (caller optionsCaller) appendStringList(options slip.List, name string, value []string) slip.List {
	var pv slip.Object
	if 0 < len(value) {
		slist := make(slip.List, len(value))
		for i, v := range value {
			slist[i] = slip.String(v)
		}
		pv = slist
	}
	return append(options, slip.Symbol(name), pv)
}

func (caller optionsCaller) appendFromArgs(options slip.List, name string, args slip.List) slip.List {
	var pv slip.Object
	key := slip.Symbol(name)
	pv, _ = slip.GetArgsKeyValue(args, key)

	return append(options, key, pv)
}
