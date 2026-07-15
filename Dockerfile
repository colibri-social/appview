# ── Build stage ───────────────────────────────────────────────────────────────
FROM rust:1.93-slim-bookworm AS builder

RUN apt-get update && apt-get install -y pkg-config libssl-dev build-essential python3 python3-pip && rm -rf /var/lib/apt/lists/*

RUN rustup component add rustfmt

ENV PIP_BREAK_SYSTEM_PACKAGES=1

WORKDIR /app

COPY . .

# Optional: stamp the exact release version into the binary (reported by
# social.colibri.server.describeServer). Unset for local builds -> falls back
# to the crate version in Cargo.toml.
ARG APPVIEW_VERSION
ENV APPVIEW_VERSION=${APPVIEW_VERSION}

RUN cargo build --release

# ── Runtime stage ─────────────────────────────────────────────────────────────
FROM debian:bookworm-slim AS runtime

RUN apt-get update && apt-get install -y ca-certificates && rm -rf /var/lib/apt/lists/* \
    && useradd --system --no-create-home --shell /usr/sbin/nologin appview

WORKDIR /app

COPY --from=builder --chown=appview:appview /app/target/release/colibri-appview ./colibri-appview

USER appview

EXPOSE 8000

CMD ["./colibri-appview"]
