# colibri-appview

An [ATProto](https://atproto.com/) appview for the **Colibri** social platform, built with [Rocket.rs](https://rocket.rs/).

## What it does

| Feature | Details |
|---------|---------|
| **Jetstream consumer** | Connects to the BlueSky Jetstream WebSocket and ingests `social.colibri.message` events into PostgreSQL in real time, with automatic reconnect. |
| **Client subscriptions** | Clients connect via WebSocket (`/api/subscribe`) and subscribe to filtered event streams — messages by channel, user-status changes by DID, etc. Easy to extend with new event types. |
| **WebRTC signaling** | Clients connect via WebSocket (`/api/webrtc/signal`) to join rooms and exchange SDP offers/answers and ICE candidates for peer-to-peer voice/video. |
| **REST API** | `GET /api/messages` — paginated message history for a given channel. |

---

## ATProto lexicon: `social.colibri.message`

| Field | Type | Description |
|-------|------|-------------|
| `$type` | string | `social.colibri.message` |
| `text` | string | Message body |
| `createdAt` | datetime | ISO 8601 |
| `channel` | string | Channel identifier |

The record key (`rkey`) and author DID are supplied by ATProto.

---

## Setup

### 1. Prerequisites

- Rust (stable, 1.75+)
- PostgreSQL 14+

### 2. Database

```sql
CREATE DATABASE colibri;
```

Then run the migration:

```bash
psql -d colibri -f migrations/001_initial.sql
```

### 3. Environment

```bash
cp .env.example .env
# Edit .env and set DATABASE_URL
```

### 4. Run

```bash
cargo run
```

The server listens on `0.0.0.0:8000` by default (override with `ROCKET_PORT` / `ROCKET_ADDRESS` in `.env`).

---

## WebSocket: client subscriptions — `/api/subscribe`

Connect and send JSON subscription requests:

```json
// Subscribe to all messages in a channel
{"action":"subscribe","event_type":"message","channel":"general"}

// Subscribe to messages in all channels
{"action":"subscribe","event_type":"message"}

// Subscribe to status changes for a specific user
{"action":"subscribe","event_type":"user_status","did":"did:plc:…"}

// Subscribe to all user-status changes
{"action":"subscribe","event_type":"user_status"}

// Unsubscribe
{"action":"unsubscribe","event_type":"message","channel":"general"}
```

The server sends events as JSON:

```json
// New message
{"type":"message","id":"…","rkey":"…","author_did":"did:plc:…","text":"Hello","channel":"general","created_at":"…","indexed_at":"…"}

// User-status change
{"type":"user_status","did":"did:plc:…","status":"online","updated_at":"…"}
```

### Extending the event system

1. Add a new variant to `AppEvent` in `src/events.rs`.
2. Add matching subscription fields to `Subscriptions` in `src/ws_handler.rs`.
3. Broadcast the event wherever it originates (e.g. `src/jetstream.rs`).

---

## WebSocket: WebRTC signaling — `/api/webrtc/signal`

Room-based signaling for peer-to-peer voice/video.

```json
// Join a room
{"type":"join","room":"my-room","peer_id":"alice"}

// Send an SDP offer to a specific peer
{"type":"offer","room":"my-room","target_peer_id":"bob","sdp":"…"}

// Send an SDP answer
{"type":"answer","room":"my-room","target_peer_id":"alice","sdp":"…"}

// Send an ICE candidate
{"type":"ice","room":"my-room","target_peer_id":"bob","candidate":"…"}

// Leave the room
{"type":"leave","room":"my-room"}
```

The server relays offers, answers, and ICE candidates between peers, and notifies all room members when a peer joins or leaves.

---

## REST API

### `GET /api/messages`

| Parameter | Required | Description |
|-----------|----------|-------------|
| `channel` | ✅ | Channel name |
| `limit`   | ❌ | 1–100, default 50 |
| `before`  | ❌ | ISO 8601 cursor for pagination |

```bash
curl "http://localhost:8000/api/messages?channel=general&limit=20"
```

---

## Environment variables

| Variable | Default | Description |
|----------|---------|-------------|
| `DATABASE_URL` | — | PostgreSQL connection string (required) |
| `ROCKET_ADDRESS` | `127.0.0.1` | Bind address |
| `ROCKET_PORT` | `8000` | Bind port |
| `JETSTREAM_URL` | `wss://jetstream2.us-east.bsky.network/subscribe?…` | Override Jetstream endpoint |
| `RUST_LOG` | `colibri_appview=info,rocket=info` | Log filter |
