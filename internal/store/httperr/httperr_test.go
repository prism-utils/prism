package httperr_test

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/prism-utils/prism/internal/store/httperr"
)

func TestIsCanceled(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name string
		err  error
		want bool
	}{
		{name: "nil", err: nil, want: false},
		{name: "canceled", err: context.Canceled, want: true},
		{name: "wrapped canceled", err: fmt.Errorf("sandbox: %w", context.Canceled), want: true},
		{name: "deadline", err: context.DeadlineExceeded, want: false},
		{name: "wrapped deadline", err: fmt.Errorf("sandbox: %w", context.DeadlineExceeded), want: false},
		{name: "unexpected eof", err: io.ErrUnexpectedEOF, want: true},
		{name: "wrapped unexpected eof", err: fmt.Errorf("abort: %w", io.ErrUnexpectedEOF), want: true},
		{name: "net closed", err: net.ErrClosed, want: true},
		{name: "generic", err: errors.New("boom"), want: false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := httperr.IsCanceled(tc.err); got != tc.want {
				t.Fatalf("IsCanceled(%v) = %v, want %v", tc.err, got, tc.want)
			}
		})
	}
}

func TestWriteClientClosed(t *testing.T) {
	t.Parallel()
	rec := httptest.NewRecorder()
	httperr.Write(rec)
	if rec.Code != httperr.StatusClientClosed || rec.Code != 499 {
		t.Fatalf("status = %d, want 499", rec.Code)
	}
	if !strings.Contains(rec.Body.String(), httperr.ClientClosedBody) {
		t.Fatalf("body = %q, want %q", rec.Body.String(), httperr.ClientClosedBody)
	}
	if rec.Body.String() == "" {
		t.Fatal("empty body")
	}
}
