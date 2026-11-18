FROM rust:1.85-alpine AS builder
RUN apk add --no-cache musl-dev
WORKDIR /src
COPY . .
RUN cargo build --release --bins

FROM alpine:3.21
RUN adduser -D -u 10001 titandb && mkdir -p /data && chown titandb:titandb /data
USER titandb
WORKDIR /data
COPY --from=builder /src/target/release/titandb /usr/local/bin/titandb
COPY --from=builder /src/target/release/titan-cli /usr/local/bin/titan-cli
EXPOSE 7440 7441
HEALTHCHECK --interval=30s --timeout=5s CMD wget -qO- http://127.0.0.1:7441/health || exit 1
ENTRYPOINT ["/usr/local/bin/titandb"]
CMD ["--data-dir", "/data/db", "--wire-addr", "0.0.0.0:7440", "--http-addr", "0.0.0.0:7441"]
