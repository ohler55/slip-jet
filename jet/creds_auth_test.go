package jet_test

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/nats-io/jwt/v2"
	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nkeys"
	"github.com/ohler55/ojg/tt"
	"github.com/ohler55/slip/sliptest"
)

// startJWTServer boots an in-process NATS server
func startJWTServer(t *testing.T) (url, credsPath string) {
	t.Helper()

	okp, err := nkeys.CreateOperator()
	tt.Nil(t, err)
	opub, _ := okp.PublicKey()

	// Create system account
	syskp, _ := nkeys.CreateAccount()
	syspub, _ := syskp.PublicKey()
	sysClaims := jwt.NewAccountClaims(syspub)
	sysClaims.Name = "SYS"
	sysJWT, err := sysClaims.Encode(okp)
	tt.Nil(t, err)

	// Application account, JetStream enabled.
	akp, _ := nkeys.CreateAccount()
	apub, _ := akp.PublicKey()
	acc := jwt.NewAccountClaims(apub)
	acc.Name = "APP"
	acc.Limits.DiskStorage = -1
	acc.Limits.MemoryStorage = -1
	accJWT, err := acc.Encode(okp)
	tt.Nil(t, err)

	// User in the application account (default perms allow all).
	ukp, _ := nkeys.CreateUser()
	upub, _ := ukp.PublicKey()
	uc := jwt.NewUserClaims(upub)
	uc.Name = "test"
	userJWT, err := uc.Encode(akp)
	tt.Nil(t, err)
	useed, _ := ukp.Seed()
	creds, err := jwt.FormatUserConfig(userJWT, useed)
	tt.Nil(t, err)

	credsPath = filepath.Join(t.TempDir(), "test.creds")
	tt.Nil(t, os.WriteFile(credsPath, creds, 0600))

	res := &server.MemAccResolver{}
	tt.Nil(t, res.Store(syspub, sysJWT))
	tt.Nil(t, res.Store(apub, accJWT))

	opts := &server.Options{
		Host:            "127.0.0.1",
		Port:            availablePort(),
		JetStream:       true,
		StoreDir:        t.TempDir(),
		TrustedKeys:     []string{opub},
		AccountResolver: res,
		SystemAccount:   syspub,
		NoLog:           true,
		NoSigs:          true,
	}
	s, err := server.NewServer(opts)
	tt.Nil(t, err)
	s.Start()
	t.Cleanup(s.Shutdown)
	if !s.ReadyForConnections(5 * time.Second) {
		t.Fatal("JWT server not ready")
	}
	return s.ClientURL(), credsPath
}

// E2E test: jet-connect :user-credentials authenticates against a JWT server,
// and the same server rejects a connection that presents no credentials
func TestConnectCredsAuthenticates(t *testing.T) {
	url, credsPath := startJWTServer(t)

	// Auth is enforced: no credentials -> rejected.
	nc, err := nats.Connect(url, nats.MaxReconnects(0))
	if err == nil {
		nc.Close()
		t.Fatal("expected auth to be enforced, but a credential-less connect succeeded")
	}

	// Control: the .creds file itself is valid via nats.UserCredentials.
	nc, err = nats.Connect(url, nats.UserCredentials(credsPath), nats.MaxReconnects(0))
	tt.Nil(t, err)
	nc.Close()

	// The real assertion: slip-jet's :user-credentials option authenticates end-to-end,
	// and the authenticated client can drive JetStream (create a stream).
	(&sliptest.Function{
		Source: fmt.Sprintf(`(let ((js (jet-connect :url %q :user-credentials %q)))
                              (send js :create-or-update-stream "TEST" :subjects '("test.>") :storage :memory)
                              (send js :close)
                              js)`, url, credsPath),
		Expect: "/#<jet-client [0-9a-f]+>/",
	}).Test(t)
}
