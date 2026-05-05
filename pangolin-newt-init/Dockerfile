FROM rust:1.95-slim AS builder

ENV APP_GROUP=appgroup \
    APP_GID=1000 \
    APP_USER=appuser \
    APP_UID=1000

# Cache build dependencies
WORKDIR /build
COPY Cargo.toml .
COPY Cargo.lock ./
RUN sed -i 's|members = \[.*\]|members = ["shared", "pangolin-newt-init"]|' Cargo.toml
COPY pangolin-newt-init/Cargo.toml ./pangolin-newt-init/
COPY pangolin-newt-init/build.rs ./pangolin-newt-init/
COPY shared/Cargo.toml ./shared/
RUN mkdir -p pangolin-newt-init/src
RUN mkdir -p pangolin-newt-init/src && \
    echo 'fn main() {}' > pangolin-newt-init/src/main.rs
RUN cargo build --release --package pangolin-newt-init 2>/dev/null || true
RUN rm -f pangolin-newt-init/src/main.rs

# Build the application
COPY pangolin-newt-init/src ./pangolin-newt-init/src
COPY shared/src/ ./shared/src/
RUN cargo build --release --package pangolin-newt-init

# Create Group and User
RUN groupadd -g $APP_GID $APP_GROUP && useradd -l -u $APP_UID -g $APP_GROUP -s /sbin/nologin $APP_USER
RUN chown $APP_UID:$APP_GID /build/target/release/pangolin-newt-init

FROM gcr.io/distroless/cc-debian13:latest AS runtime

# Copy builder files
COPY --from=builder /etc/group /etc/group
COPY --from=builder /etc/passwd /etc/passwd
COPY --from=builder /build/target/release/pangolin-newt-init /app/pangolin-newt-init

# Start the application
WORKDIR /app
USER $APP_UID
ENV RUST_LOG="warn,pangolin_newt_init=info"
ENTRYPOINT ["/app/pangolin-newt-init"]
