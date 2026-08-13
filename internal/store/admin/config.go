package admin

// Config holds admin HTTP settings.
type Config struct {
	DataDir          string
	AllowedArtifacts []string
	AdminToken       string
	RoutePrefix      string
	RBACEnabled      bool
	// RunJobs mirrors process-wide RUN_JOBS. When false this node is a read
	// replica: ensure must not open or seed writable tenant engine files.
	RunJobs bool
}
