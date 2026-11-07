FROM debian:bookworm-slim AS runtime
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates curl && rm -rf /var/lib/apt/lists/*

FROM runtime AS builder
ARG TARGETARCH
RUN curl --fail --location --retry 10 --retry-all-errors https://go.dev/dl/go1.24.2.linux-${TARGETARCH}.tar.gz -o /tmp/go.tar.gz \
    && tar -xzf /tmp/go.tar.gz -C /usr/local && rm /tmp/go.tar.gz
ENV CGO_ENABLED=0 GOMAXPROCS=2 GOPATH=/go PATH=/usr/local/go/bin:$PATH
RUN go install github.com/minio/minio@RELEASE.2025-04-22T22-12-26Z

FROM runtime
COPY --from=builder /go/bin/minio /usr/local/bin/minio
EXPOSE 9000 9001
ENTRYPOINT ["minio"]
