# Build stage
FROM rust:1.84-slim AS builder

# Install build dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    pkg-config \
    libssl-dev \
    musl-tools \
    && rm -rf /var/lib/apt/lists/*

# Set working directory
WORKDIR /build

# Copy Cargo.toml from workspace root
COPY Cargo.toml .

# Copy Cargo.lock if it exists
COPY Cargo.lock ./

# Copy tailscale-ingress-controller Cargo files
COPY tailscale-ingress-controller/Cargo.toml ./tailscale-ingress-controller/
COPY tailscale-ingress-controller/Cargo.lock ./tailscale-ingress-controller/
COPY tailscale-ingress-controller/build.rs ./tailscale-ingress-controller/

# Copy shared Cargo.toml if it exists
COPY shared/Cargo.toml ./shared/
COPY shared/Cargo.lock ./shared/

# Create source directory
RUN mkdir -p tailscale-ingress-controller/src

# Create dummy source file to pre-build dependencies
RUN mkdir -p tailscale-ingress-controller/src && \
    echo 'fn main() {}' > tailscale-ingress-controller/src/main.rs

# Pre-build dependencies (cached unless Cargo.toml changes)
RUN cargo build --release --package tailscale-ingress-controller 2>/dev/null || true

# Remove dummy source and copy real source
RUN rm -f tailscale-ingress-controller/src/main.rs
COPY tailscale-ingress-controller/src ./tailscale-ingress-controller/src

COPY shared/src/ ./shared/src/

# Build the release binary
RUN cargo build --release --package tailscale-ingress-controller

# Final runtime stage - using distroless for minimal attack surface
FROM gcr.io/distroless/cc-debian12 AS runtime

# Create non-root user
RUN groupadd -g 1000 appgroup && useradd -u 1000 -g appgroup -s /sbin/nologin appuser

# Copy the binary from builder
COPY --from=builder /build/target/release/tailscale-ingress-controller /usr/local/bin/tailscale-ingress-controller

# Set ownership
RUN chown appuser:appgroup /usr/local/bin/tailscale-ingress-controller

# Set working directory
WORKDIR /home/appuser

# Switch to non-root user
USER appuser

# Entry point
ENTRYPOINT ["/usr/local/bin/tailscale-ingress-controller"]
