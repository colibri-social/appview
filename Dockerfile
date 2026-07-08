# ── Build stage ───────────────────────────────────────────────────────────────
FROM rust:1.93-slim-bookworm AS builder

RUN apt-get update && apt-get install -y pkg-config libssl-dev build-essential python3 python3-pip && rm -rf /var/lib/apt/lists/*

RUN rustup component add rustfmt

ENV PIP_BREAK_SYSTEM_PACKAGES=1

WORKDIR /app

COPY . .

RUN cargo build --release

# ── Runtime stage ─────────────────────────────────────────────────────────────
FROM debian:bookworm-slim AS runtime

RUN apt-get update && apt-get install -y ca-certificates && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY --from=builder /app/target/release/colibri-appview ./colibri-appview

EXPOSE 8000

CMD ["./colibri-appview"]
