// Copyright (c) 2024, Peter Ohler, All rights reserved.

package jet_test

import (
	"context"
	"fmt"
	"net"
	"os"
	"testing"
	"time"

	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/ohler55/ojg/tt"
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
	// acct := server.NewAccount("test")
	// acct.AddStreamExport("test.>", nil)
	var (
		err     error
		options = server.Options{
			Host:   "127.0.0.1",
			Port:   availablePort(),
			NoLog:  true,
			NoSigs: true,
			Users: []*server.User{
				// {Username: "u1", Password: "password", Account: acct},
				{Username: "u1", Password: "password"},
			},
			// Accounts:  []*server.Account{acct},
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

func createStream(t *testing.T, name, subject string) (jetstream.JetStream, jetstream.Stream) {
	options := nats.Options{
		Url:      natsURL,
		User:     "u1",
		Password: "password",
	}
	nc, err := options.Connect()
	tt.Nil(t, err)
	js, _ := jetstream.New(nc)
	cfg := jetstream.StreamConfig{
		Name:     name,
		Subjects: []string{subject},
	}
	cfg.Storage = jetstream.FileStorage
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	var stream jetstream.Stream
	stream, err = js.CreateStream(ctx, cfg)
	tt.Nil(t, err)
	tt.NotNil(t, stream)

	return js, stream
}

func cleanupTestStream(stream string) {
	options := nats.Options{
		Url:      natsURL,
		User:     "u1",
		Password: "password",
	}
	nc, _ := options.Connect()
	js, _ := jetstream.New(nc)
	_ = js.DeleteStream(context.Background(), stream)
}
