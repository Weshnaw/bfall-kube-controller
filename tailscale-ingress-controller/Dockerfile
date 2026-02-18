FROM rust:1.84-slim AS builder

ENV APP_GROUP=appgroup \
    APP_GID=1000 \
    APP_USER=appuser \
    APP_UID=1000

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
RUN groupadd -g $APP_GID $APP_GROUP && useradd -u $APP_UID -g $APP_GROUP -s /sbin/nologin $APP_USER
RUN chown $APP_UID:$APP_GID /build/target/release/tailscale-ingress-controller

FROM gcr.io/distroless/cc-debian12 AS runtime

COPY --from=builder /etc/group /etc/group
COPY --from=builder /etc/passwd /etc/passwd
COPY --from=builder /build/target/release/tailscale-ingress-controller /app/tailscale-ingress-controller
WORKDIR /app
USER $APP_UID
ENTRYPOINT ["/app/tailscale-ingress-controller"]
