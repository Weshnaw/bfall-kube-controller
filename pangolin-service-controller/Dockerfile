FROM rust:1.93-slim AS builder

ENV APP_GROUP=appgroup \
    APP_GID=1000 \
    APP_USER=appuser \
    APP_UID=1000

# Cache build dependencies
WORKDIR /build
COPY Cargo.toml .
COPY Cargo.lock ./
COPY pangolin-service-controller/Cargo.toml ./pangolin-service-controller/
COPY pangolin-service-controller/build.rs ./pangolin-service-controller/
COPY shared/Cargo.toml ./shared/
RUN mkdir -p pangolin-service-controller/src
RUN mkdir -p pangolin-service-controller/src && \
    echo 'fn main() {}' > pangolin-service-controller/src/main.rs
RUN cargo build --release --package pangolin-service-controller 2>/dev/null || true
RUN rm -f pangolin-service-controller/src/main.rs

# Build the application
COPY pangolin-service-controller/src ./pangolin-service-controller/src
COPY shared/src/ ./shared/src/
RUN cargo build --release --package pangolin-service-controller

# Create Group and User
RUN groupadd -g $APP_GID $APP_GROUP && useradd -l -u $APP_UID -g $APP_GROUP -s /sbin/nologin $APP_USER
RUN chown $APP_UID:$APP_GID /build/target/release/pangolin-service-controller

FROM gcr.io/distroless/cc-debian13:latest AS runtime

# Copy builder files
COPY --from=builder /etc/group /etc/group
COPY --from=builder /etc/passwd /etc/passwd
COPY --from=builder /build/target/release/pangolin-service-controller /app/pangolin-service-controller

# Start the application
WORKDIR /app
USER $APP_UID
ENV RUST_LOG="warn,pangolin_service_controller=info"
ENTRYPOINT ["/app/pangolin-service-controller"]
