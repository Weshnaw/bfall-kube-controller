FROM rust:1.95-slim AS builder

ENV APP_GROUP=appgroup \
    APP_GID=1000 \
    APP_USER=appuser \
    APP_UID=1000

# Cache build dependencies
WORKDIR /build
COPY Cargo.toml .
COPY Cargo.lock ./
RUN sed -i 's|members = \[.*\]|members = ["shared", "pangolin-gateway-controller"]|' Cargo.toml
COPY pangolin-gateway-controller/Cargo.toml ./pangolin-gateway-controller/
COPY pangolin-gateway-controller/build.rs ./pangolin-gateway-controller/
COPY shared/Cargo.toml ./shared/
RUN mkdir -p pangolin-gateway-controller/src
RUN mkdir -p pangolin-gateway-controller/src && \
    echo 'fn main() {}' > pangolin-gateway-controller/src/main.rs
RUN cargo build --release --package pangolin-gateway-controller 2>/dev/null || true
RUN rm -f pangolin-gateway-controller/src/main.rs

# Build the application
COPY pangolin-gateway-controller/src ./pangolin-gateway-controller/src
COPY shared/src/ ./shared/src/
RUN cargo build --release --package pangolin-gateway-controller

# Create Group and User
RUN groupadd -g $APP_GID $APP_GROUP && useradd -l -u $APP_UID -g $APP_GROUP -s /sbin/nologin $APP_USER
RUN chown $APP_UID:$APP_GID /build/target/release/pangolin-gateway-controller

FROM gcr.io/distroless/cc-debian13:latest AS runtime

# Copy builder files
COPY --from=builder /etc/group /etc/group
COPY --from=builder /etc/passwd /etc/passwd
COPY --from=builder /build/target/release/pangolin-gateway-controller /app/pangolin-gateway-controller

# Start the application
WORKDIR /app
USER $APP_UID
ENV RUST_LOG="warn,pangolin_gateway_controller=info"
ENTRYPOINT ["/app/pangolin-gateway-controller"]
