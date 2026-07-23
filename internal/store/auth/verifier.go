package auth

import "context"

// Verifier validates bearer credentials and returns the authenticated principal.
type Verifier interface {
	Verify(ctx context.Context, rawToken string) (principal string, err error)
}
