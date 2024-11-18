// Copyright (c) 2024, Peter Ohler, All rights reserved.

package jet_test

import (
	"fmt"
	"net"
	"os"
	"testing"
	"time"

	"github.com/nats-io/nats-server/v2/server"
)

var (
	natsURL string
)

func TestMain(m *testing.M) {
	status := wrapRun(m)

	os.Exit(status)
}

func wrapRun(m *testing.M) (status int) {
	var jss *server.Server
	jss, natsURL = startJetStreamServer()
	defer func() {
		if rec := recover(); rec != nil {
			fmt.Printf("*-*-* panic: %s\n", rec)
			status = 1
		}
		if jss != nil {
			defer jss.Shutdown()
		}
	}()

	status = m.Run()

	return
}

// TBD add accounts
//  create account with function so the various options can be added
//  create users

func startJetStreamServer() (jss *server.Server, ju string) {
	var (
		err     error
		options = server.Options{
			Host:   "127.0.0.1",
			Port:   availablePort(),
			NoLog:  true,
			NoSigs: true,
			Users: []*server.User{
				{Username: "u1", Password: "password"},
			},
			JetStream: true,
			StoreDir:  "nats-store",
		}
	)
	_ = os.RemoveAll(options.StoreDir) // start with a clean slate
	if jss, err = server.NewServer(&options); err != nil {
		panic(err)
	}
	jss.Start()
	ju = jss.ClientURL()

	if !jss.ReadyForConnections(time.Second * 5) {
		panic(fmt.Sprintf("failed to connect to JetStream server on %s", ju))
	}
	return
}

func availablePort() int {
	addr, err := net.ResolveTCPAddr("tcp", "localhost:0")
	if err != nil {
		panic(err)
	}
	var listener *net.TCPListener
	if listener, err = net.ListenTCP("tcp", addr); err != nil {
		panic(err)
	}
	defer listener.Close()

	return listener.Addr().(*net.TCPAddr).Port
}
