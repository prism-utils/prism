package ingest

import (
	"bytes"
	"context"
	"crypto/x509"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"strings"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/flight"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/compress"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"

	"github.com/elk-utilities/prism/internal/duckdbfile"
	"github.com/elk-utilities/prism/internal/store/engine"
	"github.com/elk-utilities/prism/internal/tlsconf"
)

// FlightServer receives DoPut streams and lands Parquet or DuckDB windows via the engine.
type FlightServer struct {
	flight.BaseFlightServer
	cfg  Config
	eng  *engine.Engine
	log  *slog.Logger
	mem  memory.Allocator
	auth *Authenticator
	tls  *tlsconf.Config
}

// FlightOption customizes a Flight receiver.
type FlightOption func(*FlightServer)

// WithFlightTLS enables server-side TLS for the Flight listener.
func WithFlightTLS(c *tlsconf.Config) FlightOption {
	return func(s *FlightServer) { s.tls = c }
}

// NewFlightServer builds a Flight ingest receiver backed by eng.
func NewFlightServer(cfg *Config, eng *engine.Engine, log *slog.Logger, opts ...FlightOption) (*FlightServer, error) {
	if eng == nil {
		return nil, fmt.Errorf("ingest: flight: engine required")
	}
	if log == nil {
		log = slog.Default()
	}
	s := &FlightServer{
		cfg:  *cfg,
		eng:  eng,
		log:  log,
		mem:  memory.DefaultAllocator,
		auth: NewAuthenticator(cfg),
	}
	for _, opt := range opts {
		opt(s)
	}
	return s, nil
}

// Serve binds a Flight server to addr and blocks until ctx is cancelled.
func (s *FlightServer) Serve(ctx context.Context, addr string, ready func(bound string)) error {
	var mw []flight.ServerMiddleware
	if s.cfg.AuthMode == AuthBearer && s.cfg.IngestToken != "" {
		//nolint:contextcheck // gRPC stream interceptor propagates context via ServerStream
		mw = append(mw, flight.ServerMiddleware{Stream: flightBearerInterceptor(s.cfg.IngestToken)})
	}
	var grpcOpts []grpc.ServerOption
	if s.tls != nil {
		tlsCfg, err := s.tls.Build()
		if err != nil {
			return fmt.Errorf("ingest: flight tls: %w", err)
		}
		grpcOpts = append(grpcOpts, grpc.Creds(credentials.NewTLS(tlsCfg)))
	}
	srv := flight.NewServerWithMiddleware(mw, grpcOpts...)
	if err := srv.Init(addr); err != nil {
		return fmt.Errorf("ingest: flight init %q: %w", addr, err)
	}
	srv.RegisterFlightService(s)
	if ready != nil {
		ready(srv.Addr().String())
	}
	go func() {
		<-ctx.Done()
		srv.Shutdown()
	}()
	if err := srv.Serve(); err != nil && !errors.Is(err, grpc.ErrServerStopped) {
		return fmt.Errorf("ingest: flight serve: %w", err)
	}
	return nil
}

func flightBearerInterceptor(token string) grpc.StreamServerInterceptor {
	return func(srv any, ss grpc.ServerStream, _ *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		md, _ := metadata.FromIncomingContext(ss.Context())
		for _, v := range md.Get("authorization") {
			if BearerEquals(v, token) {
				return handler(srv, ss)
			}
		}
		return status.Error(codes.Unauthenticated, "ingest: unauthorized")
	}
}

// DoPut lands either opaque duckdb bytes (format=duckdb) or Arrow IPC→Parquet.
func (s *FlightServer) DoPut(stream flight.FlightService_DoPutServer) error {
	first, err := stream.Recv()
	if err != nil {
		if errors.Is(err, io.EOF) {
			return stream.Send(&flight.PutResult{})
		}
		return status.Errorf(codes.InvalidArgument, "ingest: recv: %v", err)
	}
	var path []string
	var appMeta []byte
	if first.FlightDescriptor != nil {
		path = first.FlightDescriptor.Path
		appMeta = first.AppMetadata
	}
	if duckdbfile.FormatFromFlightMeta(appMeta, path) {
		return s.doPutDuckDB(stream, first)
	}
	return s.doPutArrow(stream, first)
}

func (s *FlightServer) doPutDuckDB(stream flight.FlightService_DoPutServer, first *flight.FlightData) error {
	tenant, artifact := pathTenantArtifact(first.FlightDescriptor)
	authOK, authTenant := s.authenticateFlight(stream.Context())
	if !authOK {
		return status.Error(codes.Unauthenticated, "ingest: unauthorized")
	}
	if authTenant != "" && authTenant != tenant {
		return status.Error(codes.PermissionDenied, "ingest: forbidden")
	}
	if !ValidateTenant(tenant) {
		return status.Error(codes.NotFound, "ingest: unknown tenant")
	}
	if !ValidateArtifact(artifact, s.cfg.AllowedArtifacts) {
		return status.Error(codes.NotFound, "ingest: unknown artifact type")
	}

	var buf bytes.Buffer
	if len(first.DataBody) > 0 {
		_, _ = buf.Write(first.DataBody)
	}
	for {
		msg, err := stream.Recv()
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return status.Errorf(codes.Internal, "ingest: read duckdb: %v", err)
		}
		if len(msg.DataBody) > 0 {
			_, _ = buf.Write(msg.DataBody)
		}
	}
	if buf.Len() == 0 {
		return stream.Send(&flight.PutResult{})
	}
	body := buf.Bytes()
	if isLogArtifact(artifact) {
		n, err := s.eng.LandLogWindow(tenant, artifact, bytes.NewReader(body))
		if err != nil {
			if errors.Is(err, engine.ErrIncompatibleDuckDBStorage) {
				return status.Errorf(codes.InvalidArgument, "ingest: %v", err)
			}
			return status.Errorf(codes.Internal, "ingest: land log: %v", err)
		}
		if n > 0 {
			s.log.Info("flight landed log duckdb window", "ns", tenant, "artifact", artifact, "bytes", n)
		}
		return stream.Send(&flight.PutResult{})
	}
	n, err := s.eng.IngestDuckDB(tenant, bytes.NewReader(body))
	if err != nil {
		if errors.Is(err, engine.ErrIncompatibleDuckDBStorage) {
			return status.Errorf(codes.InvalidArgument, "ingest: %v", err)
		}
		return status.Errorf(codes.Internal, "ingest: land duckdb: %v", err)
	}
	if n > 0 {
		s.log.Info("flight ingested duckdb", "ns", tenant, "artifact", artifact, "rows", n)
	}
	return stream.Send(&flight.PutResult{})
}

func (s *FlightServer) doPutArrow(stream flight.FlightService_DoPutServer, first *flight.FlightData) error {
	rdr, err := flight.NewRecordReader(&prependStream{FlightService_DoPutServer: stream, first: first}, ipc.WithAllocator(s.mem))
	if err != nil {
		return status.Errorf(codes.InvalidArgument, "ingest: reader: %v", err)
	}
	defer rdr.Release()

	tenant, artifact := pathTenantArtifact(rdr.LatestFlightDescriptor())
	authOK, authTenant := s.authenticateFlight(stream.Context())
	if !authOK {
		return status.Error(codes.Unauthenticated, "ingest: unauthorized")
	}
	if authTenant != "" && authTenant != tenant {
		return status.Error(codes.PermissionDenied, "ingest: forbidden")
	}
	if !ValidateTenant(tenant) {
		return status.Error(codes.NotFound, "ingest: unknown tenant")
	}
	if !ValidateArtifact(artifact, s.cfg.AllowedArtifacts) {
		return status.Error(codes.NotFound, "ingest: unknown artifact type")
	}

	var recs []arrow.RecordBatch
	for rdr.Next() {
		rec := rdr.RecordBatch()
		rec.Retain()
		recs = append(recs, rec)
	}
	if err := rdr.Err(); err != nil {
		releaseRecords(recs)
		return status.Errorf(codes.Internal, "ingest: read: %v", err)
	}
	defer releaseRecords(recs)

	if len(recs) == 0 {
		return stream.Send(&flight.PutResult{})
	}

	parquetBytes, err := recordsToParquet(recs)
	if err != nil {
		return status.Errorf(codes.Internal, "ingest: parquet: %v", err)
	}
	if isLogArtifact(artifact) {
		n, err := s.eng.LandLogWindow(tenant, artifact, bytes.NewReader(parquetBytes))
		if err != nil {
			return status.Errorf(codes.Internal, "ingest: land log: %v", err)
		}
		if n > 0 {
			s.log.Info("flight landed log window", "ns", tenant, "artifact", artifact, "bytes", n)
		}
		return stream.Send(&flight.PutResult{})
	}
	n, err := s.eng.Ingest(tenant, bytes.NewReader(parquetBytes))
	if err != nil {
		return status.Errorf(codes.Internal, "ingest: land: %v", err)
	}
	if n > 0 {
		s.log.Info("flight ingested", "ns", tenant, "artifact", artifact, "rows", n)
	}
	return stream.Send(&flight.PutResult{})
}

// prependStream re-plays the already-received first FlightData before the rest
// of the DoPut stream so Arrow RecordReader sees a complete IPC sequence.
type prependStream struct {
	flight.FlightService_DoPutServer
	first *flight.FlightData
	done  bool
}

func (p *prependStream) Recv() (*flight.FlightData, error) {
	if !p.done {
		p.done = true
		if p.first != nil {
			msg := p.first
			p.first = nil
			return msg, nil
		}
	}
	return p.FlightService_DoPutServer.Recv()
}

func pathTenantArtifact(d *flight.FlightDescriptor) (tenant, artifact string) {
	if d == nil || len(d.Path) < 2 {
		return "", ""
	}
	return d.Path[0], d.Path[1]
}

func (s *FlightServer) authenticateFlight(ctx context.Context) (ok bool, tenant string) {
	switch s.cfg.AuthMode {
	case AuthNone:
		return true, ""
	case AuthBearer:
		if s.cfg.IngestToken == "" {
			return false, ""
		}
		return true, ""
	case AuthTrustedHeader:
		md, _ := metadata.FromIncomingContext(ctx)
		for _, v := range md.Get("x-tenant") {
			v = strings.TrimSpace(v)
			if v != "" {
				return true, v
			}
		}
		return false, ""
	case AuthMTLS:
		return flightMTLSIdentity(ctx)
	default:
		return false, ""
	}
}

func flightMTLSIdentity(ctx context.Context) (bool, string) {
	p, ok := peer.FromContext(ctx)
	if !ok {
		return false, ""
	}
	ti, ok := p.AuthInfo.(credentials.TLSInfo)
	if !ok {
		return false, ""
	}
	var leaf *x509.Certificate
	if len(ti.State.VerifiedChains) > 0 && len(ti.State.VerifiedChains[0]) > 0 {
		leaf = ti.State.VerifiedChains[0][0]
	} else if len(ti.State.PeerCertificates) > 0 {
		leaf = ti.State.PeerCertificates[0]
	}
	if leaf == nil {
		return false, ""
	}
	cn := strings.TrimSpace(leaf.Subject.CommonName)
	if cn == "" {
		return false, ""
	}
	return true, cn
}

func recordsToParquet(recs []arrow.RecordBatch) ([]byte, error) {
	var buf bytes.Buffer
	props := parquet.NewWriterProperties(parquet.WithCompression(compress.Codecs.Snappy))
	fw, err := pqarrow.NewFileWriter(recs[0].Schema(), &buf, props, pqarrow.DefaultWriterProps())
	if err != nil {
		return nil, fmt.Errorf("writer: %w", err)
	}
	for _, rec := range recs {
		if err := fw.Write(rec); err != nil {
			_ = fw.Close()
			return nil, fmt.Errorf("write: %w", err)
		}
	}
	if err := fw.Close(); err != nil {
		return nil, fmt.Errorf("close: %w", err)
	}
	return buf.Bytes(), nil
}

func releaseRecords(recs []arrow.RecordBatch) {
	for _, r := range recs {
		r.Release()
	}
}
