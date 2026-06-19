# latest non funziona su WSL, forse pacchetti troppo grandi
FROM rust:1.89-slim-bookworm AS builder

WORKDIR /app

COPY Cargo.toml Cargo.lock ./
COPY src ./src
COPY benches ./benches
RUN cargo build --release
FROM debian:bookworm-slim
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates \
    && mkdir -p /data/segments \
    && rm -rf /var/lib/apt/lists/*

COPY init.json /data/init.json
COPY --from=builder /app/target/release/aledb /usr/local/bin/aledb

EXPOSE 3000
CMD ["aledb"]