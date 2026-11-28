# syntax=docker/dockerfile:1
# Multi-stage: cargo-chef caches the dependency build; the runtime is
# distroless (glibc, CA certificates, no shell, no package manager). The
# container healthcheck is the binary itself (`rust-template healthcheck`).
# No TLS key is baked in: compose mounts one (see the `dev-certs` service).

FROM rust:1.99-trixie AS chef
RUN cargo install cargo-chef --locked --version 0.1.78
WORKDIR /build

FROM chef AS planner
COPY . .
RUN cargo chef prepare --recipe-path recipe.json

# One-shot dev certificate generator for compose: writes a self-signed pair
# into /certs unless one is already there. The rust image ships openssl.
# It runs as the app's uid: Docker seeds a new named volume from this image's
# /certs, ownership included, so the app (also uid 10001) owns the key.
FROM chef AS dev-certs
COPY certs/generate_self_signed_cert.sh /generate_self_signed_cert.sh
RUN mkdir /certs && chown 10001:10001 /certs
USER 10001
ENTRYPOINT ["bash", "-c", "[ -s /certs/key.pem ] || { bash /generate_self_signed_cert.sh /certs && chmod 0600 /certs/key.pem; }"]

FROM chef AS builder
# Profiling builds (docker-compose.profiling.yml) pass
# RUSTFLAGS="-C force-frame-pointers=yes" so perf can walk whole stacks.
# Empty, the default, is the normal release build. Set before `cargo chef cook`
# so the dependency layer is built with the same flags.
ARG RUSTFLAGS=""
ENV RUSTFLAGS=${RUSTFLAGS}
# The image carries the Solace transport and the demo publisher, so one
# image serves every compose service; INGEST_TRANSPORT still defaults to noop.
ARG CARGO_FEATURES="solace"
COPY --from=planner /build/recipe.json recipe.json
RUN cargo chef cook --release --locked --features "${CARGO_FEATURES}" --recipe-path recipe.json
COPY . .
RUN cargo build --release --locked --features "${CARGO_FEATURES}" --bins

FROM gcr.io/distroless/cc-debian13:nonroot@sha256:29ee70762214fe8190cea04ef1c0a84a8365879e047ce8f911ae5a5ab2355fa2 AS runtime
WORKDIR /app
COPY --from=builder /build/target/release/rust-template /app/rust-template
COPY --from=builder /build/target/release/demo-publisher /app/demo-publisher
USER 10001:10001
EXPOSE 8443
HEALTHCHECK --interval=5s --timeout=5s --retries=12 --start-period=10s \
  CMD ["/app/rust-template", "healthcheck"]
CMD ["/app/rust-template"]
