FROM rust:1.93-slim AS builder

ENV APP_GROUP=appgroup \
    APP_GID=1000 \
    APP_USER=appuser \
    APP_UID=1000

# Cache build dependencies
WORKDIR /build
COPY Cargo.toml .
COPY Cargo.lock ./
COPY pangolin-resource-controller/Cargo.toml ./pangolin-resource-controller/
COPY pangolin-resource-controller/build.rs ./pangolin-resource-controller/
COPY shared/Cargo.toml ./shared/
RUN mkdir -p pangolin-resource-controller/src
RUN mkdir -p pangolin-resource-controller/src && \
    echo 'fn main() {}' > pangolin-resource-controller/src/main.rs
RUN cargo build --release --package pangolin-resource-controller 2>/dev/null || true
RUN rm -f pangolin-resource-controller/src/main.rs

# Build the application
COPY pangolin-resource-controller/src ./pangolin-resource-controller/src
COPY shared/src/ ./shared/src/
RUN cargo build --release --package pangolin-resource-controller

# Create Group and User
RUN groupadd -g $APP_GID $APP_GROUP && useradd -l -u $APP_UID -g $APP_GROUP -s /sbin/nologin $APP_USER
RUN chown $APP_UID:$APP_GID /build/target/release/pangolin-resource-controller

FROM gcr.io/distroless/cc-debian13:latest AS runtime

# Copy builder files
COPY --from=builder /etc/group /etc/group
COPY --from=builder /etc/passwd /etc/passwd
COPY --from=builder /build/target/release/pangolin-resource-controller /app/pangolin-resource-controller

# Start the application
WORKDIR /app
USER $APP_UID
ENV RUST_LOG="warn,pangolin_resource_controller=info"
ENTRYPOINT ["/app/pangolin-resource-controller"]
