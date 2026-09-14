// Package buildinfo contains values injected into release binaries.
package buildinfo

import "fmt"

var (
	Version = "dev"
	Commit  = "none"
	Date    = "unknown"
)

// String returns a human-readable build identifier.
func String() string {
	return fmt.Sprintf("version=%s commit=%s date=%s", Version, Commit, Date)
}
