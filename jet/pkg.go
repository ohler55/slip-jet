// Copyright (c) 2024, Peter Ohler, All rights reserved.

package jet

import (
	"github.com/ohler55/slip"
)

var (
	// Pkg is the message package.
	Pkg = slip.Package{
		Name:      "jet",
		Nicknames: []string{"jet"},
		Doc:       "Home of symbols defined for the jet functions, variables, and constants.",
		PreSet:    slip.DefaultPreSet,
	}
)

func init() {
	Pkg.Initialize(map[string]*slip.VarVal{})
	defJetstream()
	defStream()
	defConsumer()
	defMsg()

	slip.DefConstant(slip.Symbol("*jet*"), &Pkg, "")
	Pkg.Initialize(nil, &jet{}) // lock
	slip.AddPackage(&Pkg)
	slip.UserPkg.Use(&Pkg)
}
