FROM rust:1.85-slim AS builder
WORKDIR /app
COPY . .
RUN cargo build --release

FROM debian:bookworm-slim
WORKDIR /app
COPY --from=builder /app/target/release/orderbook /app/orderbook
COPY config.toml /app/config.toml
ENTRYPOINT ["/app/orderbook"]
