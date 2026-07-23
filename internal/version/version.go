// Package version holds the build-time version string shared by prism binaries.
package version

// Version is the release identifier injected at link time via -ldflags -X.
var Version = "dev"
