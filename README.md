# appview

The AT Protocol Application View (AppView) that sits behind [colibri.social](https://colibri.social). See the docs at <https://colibri.social/docs>.

## Getting started

### Prerequisites

- Rust (`1.93`+).
- Docker + Docker Compose, for Postgres and Tap
- On **Windows**, two extra build tools for OpenSSL. See [Building on Windows](#building-on-windows).

### 1. Configure environment

Config is read from a `.env` file in the repo root. Copy `.env.example` to `.env` to create one.

### 2. Start dependencies

Bring up Postgres and Tap with the dev compose file:

```sh
docker compose -f docker-compose.dev.yml up
```

### 3. Run the AppView

Migrations run automatically on boot, so you just need:

```sh
cargo run
```

The service listens on `http://127.0.0.1:8000`. Hit `/` for the landing banner. API endpoints live under `/xrpc/`.

## Web Push (VAPID)

Background push notifications (delivered to the client's service worker when the app is closed) require a VAPID keypair. Generate one with:

```sh
npx web-push generate-vapid-keys
```

When these are unset, background Web Push is disabled.

## Building on Windows

Web Push pulls in OpenSSL. On Linux/Docker the system `libssl` is used. Windows has no system OpenSSL to link against, so `Cargo.toml` builds it from source there. That build needs two tools on your `PATH`:

- Strawberry Perl: <https://strawberryperl.com/>
- NASM: <https://www.nasm.us/> (the installer does _not_ add itself to `PATH`. Add its install folder manually)

With [Chocolatey](https://chocolatey.org/install): `choco install strawberryperl nasm -y`.

Open a fresh terminal afterwards so the updated `PATH` is picked up (`perl --version` / `nasm --version` to confirm), then `cargo run`. The first build compiles OpenSSL from source, which might take up to 15 minutes.

## Deployment

`docker-compose.yml` builds the release image (`Dockerfile`) and runs it alongside Postgres and Tap. Provide the same env vars as above.

## Tap

For configuring Tap, see the [Tap README](https://github.com/bluesky-social/indigo/blob/main/cmd/tap/README.md).
