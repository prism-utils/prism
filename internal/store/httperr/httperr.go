// Package httperr maps a gone client to the conventional non-RFC status and body.
package httperr

import (
	"context"
	"errors"
	"io"
	"net"
	"net/http"
)

// StatusClientClosed is the nginx / Grafana / Loki convention for a client that
// went away before the server finished (not an RFC status).
const StatusClientClosed = 499

// ClientClosedBody is the plain-text response body for a gone client.
const ClientClosedBody = "client closed"

// IsCanceled reports whether err is a client-gone cancel, not a deadline.
func IsCanceled(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, context.Canceled) {
		return true
	}
	if errors.Is(err, io.ErrUnexpectedEOF) || errors.Is(err, net.ErrClosed) {
		return true
	}
	return false
}

// Write sends the gone-client status and body.
func Write(w http.ResponseWriter) {
	http.Error(w, ClientClosedBody, StatusClientClosed)
}
