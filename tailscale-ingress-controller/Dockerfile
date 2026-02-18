FROM rust:1.84-slim AS builder

RUN apt-get update && apt-get install -y --no-install-recommends \
    pkg-config \
    libssl-dev \
    musl-tools \
    && rm -rf /var/lib/apt/lists/*
WORKDIR /build
COPY Cargo.toml .
COPY Cargo.lock ./
COPY tailscale-ingress-controller/Cargo.toml ./tailscale-ingress-controller/
COPY tailscale-ingress-controller/build.rs ./tailscale-ingress-controller/
COPY shared/Cargo.toml ./shared/
RUN mkdir -p tailscale-ingress-controller/src
RUN mkdir -p tailscale-ingress-controller/src && \
    echo 'fn main() {}' > tailscale-ingress-controller/src/main.rs
RUN cargo build --release --package tailscale-ingress-controller 2>/dev/null || true
RUN rm -f tailscale-ingress-controller/src/main.rs
COPY tailscale-ingress-controller/src ./tailscale-ingress-controller/src
COPY shared/src/ ./shared/src/
RUN cargo build --release --package tailscale-ingress-controller

FROM gcr.io/distroless/cc-debian12 AS runtime

RUN groupadd -g 1000 appgroup && useradd -u 1000 -g appgroup -s /sbin/nologin appuser
COPY --from=builder /build/target/release/tailscale-ingress-controller /usr/local/bin/tailscale-ingress-controller
RUN chown appuser:appgroup /usr/local/bin/tailscale-ingress-controller
WORKDIR /home/appuser
USER appuser
ENTRYPOINT ["/usr/local/bin/tailscale-ingress-controller"]
