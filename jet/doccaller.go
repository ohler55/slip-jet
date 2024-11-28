// Copyright (c) 2024, Peter Ohler, All rights reserved.

package jet

import "github.com/ohler55/slip"

// DocCaller is a caller used for documentation only.
type DocCaller struct {
	Text string
}

// Call returns nil.
func (caller *DocCaller) Call(_ *slip.Scope, _ slip.List, _ int) slip.Object {
	return nil
}

// Docs returns the documentation for the caller.
func (caller *DocCaller) Docs() string {
	return caller.Text
}
