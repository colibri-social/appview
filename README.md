# appview

The AT Protocol Application View (AppView) that sits behind [colibri.social](https://colibri.social). See the docs at <https://colibri.social/docs>. Use via <https://api.colibri.social>.

## Getting started

### Prerequisites

- Linux (`x86_64` or `aarch64`)
- Rust (`1.96`+).
- Docker + Docker Compose, for Postgres and Tap
- **A PDS you control**, plus its admin password. Communities are real AT Protocol accounts, and the AppView creates them on this PDS. See [PDS](#pds) below.

### 1. Configure environment

Config is read from a `.env` file in the repo root. Copy `.env.example` to `.env` to create one.

### 2. Start dependencies

Bring up Postgres and Tap with the dev compose file, adding the PDS overlay if you
want to create communities locally (see [PDS](#pds)):

```sh
docker compose -f docker-compose.dev.yml up -d

# ...or, with a local PDS + its own PLC directory:
docker compose -f docker-compose.dev.yml -f docker-compose.pds.dev.yml up -d
```

### 3. Run the AppView

Migrations run automatically on boot, so you just need:

```sh
cargo run
```

The service listens on `http://127.0.0.1:8000`. Hit `/` for the landing banner. API endpoints live under `/xrpc/`.

## PDS

Every community is its own AT Protocol repository: `social.colibri.community.create` mints a fresh account on a PDS at `PDS_LOC` (authenticating with `PDS_ADMIN_PASS`), writes the community's bootstrap records to it, and stores the resulting credentials encrypted. Community deletion and migration go through the same PDS. Without one, the AppView boots and serves reads, but no community can be created.

`PDS_LOC` can point at any PDS whose admin password you hold. If you don't already run one, two compose files run the official Bluesky PDS configured for this purpose.

**Production**: `docker-compose.pds.yml` is `docker-compose.yml` plus a `pds` service, so run it _instead of_ the plain file:

```sh
docker compose -f docker-compose.pds.yml up -d
```

It needs `PDS_HOSTNAME`, `PDS_JWT_SECRET` and `PDS_PLC_ROTATION_KEY` in `.env` on top of the variables the AppView itself reads (`.env.example` documents all of them, including how to generate the keys), plus two things no compose file can arrange for you:

- **TLS in front of it.** The PDS announces `https://${PDS_HOSTNAME}` in every community DID document it registers with the PLC directory, so that hostname has to serve HTTPS. The compose file publishes the PDS on `127.0.0.1:3000` by default, for a reverse proxy on the same host, and also joins `dokploy-network` so a containerised proxy can route to `pds:3000`.
- **Wildcard DNS for community handles.** Communities get handles like `c-3lk2....${APPVIEW_HANDLE_DOMAIN}`, which the PDS answers for, so `*.${APPVIEW_HANDLE_DOMAIN}` must resolve to it. The simplest correct setup is `APPVIEW_HANDLE_DOMAIN` equal to `PDS_HOSTNAME`.

**Development**: `docker-compose.pds.dev.yml` overlays the dev dependencies with a PDS on `localhost` plus its own PLC directory, so community DIDs minted while you work never touch `plc.directory`:

```sh
docker compose -f docker-compose.dev.yml -f docker-compose.pds.dev.yml up -d
```

Then set `PDS_LOC=http://localhost:3000` (and, conveniently, `APPVIEW_HANDLE_DOMAIN=test`). Creating a community works end to end: the AppView indexes its own writes, so the community is readable immediately without Tap having to backfill the repo, which it couldn't, since `http://localhost:3000` is unreachable from inside its container. Two consequences worth knowing:

- Records written to that PDS by anything other than this AppView never appear locally, and `record_data` is the only copy of the ones that do, wiping the dev database loses local test communities for good. Never set `REFILL_FROM_SCRATCH` against a local PDS.
- Community DIDs resolve only inside your stack. Anything needing public resolution (federating with another AppView, a client on another machine) still wants a public PDS or a tunnel.

If you're not working on community creation at all, any placeholder `PDS_LOC` will do. It's only read when a community is created, deleted, or migrated.

## Web Push (VAPID)

Background push notifications (delivered to the client's service worker when the app is closed) require a VAPID keypair. Generate one with:

```sh
npx web-push generate-vapid-keys
```

When these are unset, background Web Push is disabled.

## Deployment

`docker-compose.yml` pulls the published release image (`ghcr.io/colibri-social/appview:latest`, built from `Dockerfile`) and runs it alongside Postgres and Tap. Provide the same env vars as above. Add `-f docker-compose.pds.yml` if the PDS should run in the same stack.

## Tap

For configuring Tap, see the [Tap README](https://github.com/bluesky-social/indigo/blob/main/cmd/tap/README.md).
