# Multi-stage build → single static binary on a distroless nonroot base.
# The artifact is CGO-free so it runs identically on bare metal and here.

# --- build stage -----------------------------------------------------------
FROM golang:1.25 AS build
WORKDIR /src

# Cache modules first.
COPY go.mod go.sum* ./
RUN go mod download

COPY . .
ARG VERSION=dev
ENV CGO_ENABLED=0
RUN go build -trimpath -ldflags "-s -w -X main.version=${VERSION}" \
    -o /out/prism ./cmd/prism

# --- runtime stage ---------------------------------------------------------
# distroless nonroot: no shell, no package manager, runs as uid 65532.
FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/prism /usr/local/bin/prism

# Config is mounted at /etc/prism/prism.yaml; secrets come from env (${VAR}).
USER nonroot:nonroot
ENTRYPOINT ["/usr/local/bin/prism"]
CMD ["run", "-config", "/etc/prism/prism.yaml"]
