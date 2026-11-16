# syntax=docker/dockerfile:1
# Multi-stage: cargo-chef caches the dependency build; the runtime is a slim
# Debian image with a non-root user and curl for the compose healthcheck.

FROM rust:1.96-trixie AS chef
RUN cargo install cargo-chef --locked --version 0.1.78
WORKDIR /build

FROM chef AS planner
COPY . .
RUN cargo chef prepare --recipe-path recipe.json

FROM chef AS builder
COPY --from=planner /build/recipe.json recipe.json
RUN cargo chef cook --release --locked --recipe-path recipe.json
COPY . .
RUN cargo build --release --locked --bins
# Self-signed dev cert for HTTPS on 443 (openssl ships with the rust image).
RUN bash certs/generate_self_signed_cert.sh /build/certs

FROM debian:trixie-slim AS runtime
RUN apt-get update \
 && apt-get install -y --no-install-recommends ca-certificates curl \
 && rm -rf /var/lib/apt/lists/*
RUN useradd --system --uid 10001 --no-create-home --shell /usr/sbin/nologin app
WORKDIR /app
COPY --from=builder /build/target/release/rust-template /app/rust-template
COPY --from=builder /build/target/release/demo-publisher /app/demo-publisher
COPY --from=builder --chown=10001:10001 /build/certs/key.pem /build/certs/cert.pem /app/certs/
# Non-root can bind 443: Docker sets net.ipv4.ip_unprivileged_port_start=0
# inside containers (Docker >= 20.10).
USER 10001
EXPOSE 443
CMD ["/app/rust-template"]
