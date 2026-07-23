package admin

// Config holds admin HTTP settings.
type Config struct {
	DataDir          string
	AllowedArtifacts []string
	AdminToken       string
	RoutePrefix      string
	RBACEnabled      bool
}
