# appview

The AT Protocol Application View (AppView) that sits behind [colibri.social](https://colibri.social). See the docs at <https://colibri.social/docs>.

## Getting started

### Prerequisites

- Linux (`x86_64` or `aarch64`)
- Rust (`1.93`+).
- Docker + Docker Compose, for Postgres and Tap

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

## Deployment

`docker-compose.yml` builds the release image (`Dockerfile`) and runs it alongside Postgres and Tap. Provide the same env vars as above.

## Tap

For configuring Tap, see the [Tap README](https://github.com/bluesky-social/indigo/blob/main/cmd/tap/README.md).
