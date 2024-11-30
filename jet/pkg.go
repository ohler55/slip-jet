// Copyright (c) 2024, Peter Ohler, All rights reserved.

package jet

import (
	"github.com/ohler55/slip"
)

var (
	// Pkg is the jet package.
	Pkg = slip.Package{
		Name:      "jet",
		Nicknames: []string{"jet"},
		Doc:       "Home of symbols defined for the jet functions, variables, and constants.",
		PreSet:    slip.DefaultPreSet,
	}
)

func init() {
	Pkg.Initialize(map[string]*slip.VarVal{})
	defClient()
	defStream()
	defConsumer()
	defMsg()
	defAck()

	initConnect()

	slip.DefConstant(slip.Symbol("*jet*"), &Pkg, "")
	Pkg.Initialize(nil, &PubMsg{}) // lock
	slip.AddPackage(&Pkg)
	slip.UserPkg.Use(&Pkg)
}
