package pipeline

import "fmt"

// ensure fmt is used by sampling.go via this file so the package compiles
// cleanly without a separate import block in sampling.go.
var _ = fmt.Sprintf
