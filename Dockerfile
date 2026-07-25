# ── Base stage (toolchain + native build deps) ─────────────────────────────────
FROM rust:1.96-slim-bookworm AS chef

RUN apt-get update && apt-get install -y pkg-config libssl-dev build-essential python3 python3-pip && rm -rf /var/lib/apt/lists/*

RUN rustup component add rustfmt

RUN cargo install cargo-chef --locked --version 0.1.77

ENV PIP_BREAK_SYSTEM_PACKAGES=1

WORKDIR /app

# ── Planner stage ─────────────────────────────────────────────────────────────
FROM chef AS planner

COPY . .

RUN cargo chef prepare --recipe-path recipe.json

# ── Build stage ───────────────────────────────────────────────────────────────
FROM chef AS builder

COPY --from=planner /app/recipe.json recipe.json

RUN cargo chef cook --release --recipe-path recipe.json

COPY . .

# Optional: stamp the exact release version into the binary (reported by
# social.colibri.server.describeServer). Unset for local builds -> falls back
# to the crate version in Cargo.toml. Declared after `cook` so bumping it never
# invalidates the dependency layer.
ARG APPVIEW_VERSION
ENV APPVIEW_VERSION=${APPVIEW_VERSION}

RUN cargo build --release --locked

# ── Runtime stage ─────────────────────────────────────────────────────────────
FROM debian:bookworm-slim AS runtime

RUN apt-get update && apt-get install -y ca-certificates && rm -rf /var/lib/apt/lists/* \
    && useradd --system --no-create-home --shell /usr/sbin/nologin appview

WORKDIR /app

COPY --from=builder --chown=appview:appview /app/target/release/colibri-appview ./colibri-appview

USER appview

EXPOSE 8000

CMD ["./colibri-appview"]
