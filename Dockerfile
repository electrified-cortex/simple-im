# Multi-stage Rust build for simple-im
FROM rust:1-alpine AS builder
RUN apk add --no-cache musl-dev
WORKDIR /app
COPY Cargo.toml Cargo.lock ./
# Cache deps layer
RUN mkdir src && echo "fn main(){}" > src/main.rs && cargo build --release --target x86_64-unknown-linux-musl && rm -rf src
COPY src ./src
COPY tests ./tests
COPY skills ./skills
COPY docs ./docs
RUN touch src/main.rs && cargo build --release --target x86_64-unknown-linux-musl

FROM alpine:3.20
RUN apk add --no-cache ca-certificates
WORKDIR /app
COPY --from=builder /app/target/x86_64-unknown-linux-musl/release/simple-im /app/simple-im
EXPOSE 8080
ENV SIMPLE_IM_INSECURE_HTTP=1
CMD ["/app/simple-im", "--insecure-http", "--port", "8080"]
