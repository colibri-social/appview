# colibri-appview

An [ATProto](https://atproto.com/) appview for the **Colibri** social platform, built with [Rocket.rs](https://rocket.rs/).

## Overview

| Feature | Details |
|---------|---------|
| **Jetstream consumer** | Connects to a Colibri Jetstream WebSocket and ingests all Colibri lexicon records into PostgreSQL in real time, with cursor persistence and automatic reconnect. |
| **Backfill** | On startup, fetches historical records from each known DID's PDS so no data is missed. Triggered again automatically when the appview falls behind. |
| **Client subscriptions** | Clients connect via WebSocket (`/api/subscribe`) and subscribe to filtered event streams — messages by channel, community events by AT-URI. Easy to extend with new event types. |
| **WebRTC signaling** | Room-based peer-to-peer voice/video signaling over WebSocket (`/api/webrtc/signal`). |
| **REST API** | Full set of read endpoints for messages, authors, reactions, communities, channels, categories, members, and invite codes. |

---

## Setup

### Prerequisites

- Rust (stable, 1.75+)
- PostgreSQL 14+

### 1. Database

```bash
createdb colibri
```

Migrations run automatically on startup via `sqlx::migrate!`. No manual SQL needed.

### 2. Environment

```bash
cp .env.example .env
# Fill in at minimum DATABASE_URL and INVITE_API_KEY
```

### 3. Run

```bash
cargo run
```

The server listens on `0.0.0.0:8000` by default.

### Docker

```bash
docker compose up --build
# or for development with live reload:
docker compose -f docker-compose.dev.yml up
```

---

## Environment variables

| Variable | Required | Default | Description |
|----------|----------|---------|-------------|
| `DATABASE_URL` | ✅ | — | PostgreSQL connection string, e.g. `postgres://user:pass@localhost:5432/colibri` |
| `INVITE_API_KEY` | ✅ | — | Bearer token required for `POST /api/invite` and `DELETE /api/invite/<code>` |
| `ROCKET_ADDRESS` | ❌ | `0.0.0.0` | Bind address |
| `ROCKET_PORT` | ❌ | `8000` | Bind port |
| `JETSTREAM_URL` | ❌ | `wss://jetstream2.us-east.bsky.network/subscribe?…` | Override Jetstream endpoint (e.g. point at a self-hosted instance) |
| `RUST_LOG` | ❌ | `colibri_appview=info,rocket=info` | Log filter (`trace`, `debug`, `info`, `warn`, `error`) |

---

## REST API

All endpoints return JSON. CORS is open (`*`).

### Messages

#### `GET /api/messages`

Paginated message history for a channel, newest first. Each message includes the author profile.

| Parameter | Required | Description |
|-----------|----------|-------------|
| `channel` | ✅ | Channel rkey |
| `limit` | ❌ | 1–100, default 50 |
| `before` | ❌ | ISO 8601 timestamp — returns messages older than this (pagination cursor) |

```bash
curl "http://localhost:8000/api/messages?channel=general&limit=20"
curl "http://localhost:8000/api/messages?channel=general&before=2024-03-01T12:00:00Z"
```

#### `GET /api/message`

Fetch a single message by author and record key.

| Parameter | Required | Description |
|-----------|----------|-------------|
| `author` | ✅ | Author DID |
| `rkey` | ✅ | Record key |

```bash
curl "http://localhost:8000/api/message?author=did:plc:xxx&rkey=3mxxx"
```

### Authors

#### `GET /api/authors`

Retrieve a cached author profile. Falls back to a live ATProto fetch if not yet cached.

| Parameter | Required | Description |
|-----------|----------|-------------|
| `did` | ✅ | Author DID |

```bash
curl "http://localhost:8000/api/authors?did=did:plc:xxx"
```

**Response:**
```json
{
  "did": "did:plc:xxx",
  "display_name": "Alice",
  "avatar_url": "https://cdn.bsky.app/..."
}
```

### Reactions

#### `GET /api/reactions`

Reactions for a single message, grouped by emoji with reactor DIDs.

| Parameter | Required | Description |
|-----------|----------|-------------|
| `message` | ✅ | Target message rkey |

#### `GET /api/reactions/channel`

All reactions in a channel, keyed by target message rkey.

| Parameter | Required | Description |
|-----------|----------|-------------|
| `channel` | ✅ | Channel rkey |

**Response:**
```json
{
  "3mxxx": [{ "emoji": "👍", "count": 3, "reactor_dids": ["did:plc:..."] }],
  "3myyy": [{ "emoji": "❤️", "count": 1, "reactor_dids": ["did:plc:..."] }]
}
```

### Communities

#### `GET /api/communities`

All communities for a user — both owned and joined — in a single roundtrip.

| Parameter | Required | Description |
|-----------|----------|-------------|
| `did` | ✅ | User DID |

**Response:**
```json
{
  "owned": [
    {
      "owner_did": "did:plc:xxx",
      "rkey": "3mxxx",
      "name": "My Community",
      "description": "...",
      "image": { ... },
      "category_order": [ ... ]
    }
  ],
  "joined": [ ... ]
}
```

#### `GET /api/channels`

All channels for a community.

| Parameter | Required | Description |
|-----------|----------|-------------|
| `community` | ✅ | Community AT-URI (`at://did:plc:xxx/social.colibri.community/rkey`) |

**Response:** array of channel objects with `uri`, `rkey`, `name`, `description`, `channel_type`, `category_rkey`.

#### `GET /api/sidebar`

Channels and categories combined into a sidebar-ready structure. Categories nest their channels; channels with no category appear under `uncategorized`.

| Parameter | Required | Description |
|-----------|----------|-------------|
| `community` | ✅ | Community AT-URI |

**Response:**
```json
{
  "categories": [
    {
      "uri": "at://...",
      "rkey": "3mxxx",
      "name": "General",
      "emoji": "💬",
      "parent_rkey": null,
      "channels": [
        { "uri": "...", "rkey": "...", "name": "announcements", "channel_type": "text", "category_rkey": "3mxxx" }
      ]
    }
  ],
  "uncategorized": [
    { "uri": "...", "rkey": "...", "name": "off-topic", "channel_type": "text", "category_rkey": null }
  ]
}
```

#### `GET /api/members`

All members of a community, enriched with cached profile data. The owner is always included with `status: "owner"`.

| Parameter | Required | Description |
|-----------|----------|-------------|
| `community` | ✅ | Community AT-URI |

**Response:**
```json
[
  { "member_did": "did:plc:xxx", "status": "owner", "display_name": "Alice", "avatar_url": "..." },
  { "member_did": "did:plc:yyy", "status": "approved", "display_name": "Bob", "avatar_url": null },
  { "member_did": "did:plc:zzz", "status": "pending", "display_name": null, "avatar_url": null }
]
```

### Invite codes

#### `GET /api/invite/<code>`

Look up an invite code and return the associated community.

**Response:**
```json
{
  "code": "abc123",
  "community_uri": "at://...",
  "created_by_did": "did:plc:xxx",
  "max_uses": 10,
  "use_count": 3,
  "active": true,
  "community": { ... }
}
```

#### `POST /api/invite` 🔒

Create a new invite code. Requires `Authorization: Bearer <INVITE_API_KEY>`.

**Body:**
```json
{ "community_uri": "at://...", "owner_did": "did:plc:xxx", "max_uses": 10 }
```
`max_uses` may be `null` for unlimited.

**Response:** `{ "code": "abc123" }`

#### `DELETE /api/invite/<code>?owner_did=<did>` 🔒

Revoke (deactivate) an invite code. Requires `Authorization: Bearer <INVITE_API_KEY>`.

Returns `204 No Content` on success, `403 Forbidden` if the `owner_did` doesn't match.

---

## WebSocket: real-time events — `GET /api/subscribe`

Connect with any WebSocket client. Send JSON subscription requests; receive JSON events.

### Subscribing

```json
// Messages in a specific channel
{ "action": "subscribe", "event_type": "message", "channel": "<channel-rkey>" }

// Messages in all channels
{ "action": "subscribe", "event_type": "message" }

// All events for a specific community (channels, categories, members, community metadata)
{ "action": "subscribe", "event_type": "community", "community_uri": "at://did:plc:xxx/social.colibri.community/<rkey>" }

// Unsubscribing works the same way
{ "action": "unsubscribe", "event_type": "message", "channel": "<channel-rkey>" }
{ "action": "unsubscribe", "event_type": "community", "community_uri": "at://..." }

// Keepalive (server replies with empty ack)
{ "action": "heartbeat" }
```

Multiple subscriptions are cumulative. You can subscribe to several channels and/or several communities at once.

### Events — `message` subscription

Delivered to clients subscribed to the matching channel.

#### `message`
A new message was posted (or an existing one updated). Includes full author profile.

```json
{
  "type": "message",
  "message": {
    "id": "uuid",
    "rkey": "3mxxx",
    "author_did": "did:plc:xxx",
    "text": "Hello!",
    "channel": "general",
    "created_at": "2024-03-01T12:00:00Z",
    "indexed_at": "2024-03-01T12:00:01Z"
  },
  "author": { "did": "...", "display_name": "Alice", "avatar_url": "..." },
  "parent": null
}
```

#### `message_deleted`
```json
{ "type": "message_deleted", "id": "uuid", "rkey": "3mxxx", "author_did": "did:plc:xxx", "channel": "general" }
```

#### `reaction_added`
```json
{
  "type": "reaction_added",
  "rkey": "3mxxx",
  "author_did": "did:plc:xxx",
  "emoji": "👍",
  "target_rkey": "3myyy",
  "target_author_did": "did:plc:yyy",
  "channel": "general"
}
```

#### `reaction_removed`
Same shape as `reaction_added`.

---

### Events — `community` subscription

Delivered to clients subscribed to the matching `community_uri`. Subscribe to a community to receive all of the following.

#### `community_upserted`
A community was created or its metadata updated.
```json
{
  "type": "community_upserted",
  "community_uri": "at://did:plc:xxx/social.colibri.community/3mxxx",
  "owner_did": "did:plc:xxx",
  "rkey": "3mxxx",
  "name": "My Community",
  "description": "...",
  "image": { ... },
  "category_order": [ ... ]
}
```

#### `community_deleted`
```json
{ "type": "community_deleted", "community_uri": "at://...", "owner_did": "did:plc:xxx", "rkey": "3mxxx" }
```

#### `channel_created`
A channel was created or updated.
```json
{
  "type": "channel_created",
  "community_uri": "at://...",
  "uri": "at://did:plc:xxx/social.colibri.channel/3mxxx",
  "rkey": "3mxxx",
  "name": "announcements",
  "channel_type": "text",
  "category_rkey": "3mcat"
}
```

#### `channel_deleted`
```json
{ "type": "channel_deleted", "community_uri": "at://...", "uri": "at://...", "rkey": "3mxxx" }
```

#### `category_created`
A category was created or updated.
```json
{
  "type": "category_created",
  "community_uri": "at://...",
  "uri": "at://did:plc:xxx/social.colibri.category/3mxxx",
  "rkey": "3mxxx",
  "name": "General",
  "emoji": "💬",
  "parent_rkey": null
}
```

#### `category_deleted`
```json
{ "type": "category_deleted", "community_uri": "at://...", "uri": "at://...", "rkey": "3mxxx" }
```

#### `member_pending`
A user requested to join (membership record created, awaiting approval).
```json
{ "type": "member_pending", "community_uri": "at://...", "member_did": "did:plc:yyy", "membership_uri": "at://..." }
```

#### `member_joined`
A user was approved and is now a full member.
```json
{ "type": "member_joined", "community_uri": "at://...", "member_did": "did:plc:yyy", "membership_uri": "at://..." }
```

#### `member_left`
A membership record was deleted (user left or was removed).
```json
{ "type": "member_left", "community_uri": "at://...", "member_did": "did:plc:yyy" }
```

---

## WebSocket: WebRTC signaling — `GET /api/webrtc/signal`

Room-based peer-to-peer voice/video signaling. The server relays SDP offers/answers and ICE candidates between peers.

```json
// Join a room
{ "type": "join", "room": "my-room", "peer_id": "alice" }

// Send an SDP offer to a specific peer
{ "type": "offer", "room": "my-room", "target_peer_id": "bob", "sdp": "..." }

// Send an SDP answer
{ "type": "answer", "room": "my-room", "target_peer_id": "alice", "sdp": "..." }

// Send an ICE candidate
{ "type": "ice", "room": "my-room", "target_peer_id": "bob", "candidate": "..." }

// Leave the room
{ "type": "leave", "room": "my-room" }
```

The server notifies all room members when a peer joins or leaves.

---

## ATProto lexicons consumed

| Collection | Description |
|------------|-------------|
| `social.colibri.message` | Chat messages (`text`, `channel`, `createdAt`, optional `parent`, `facets`) |
| `social.colibri.reaction` | Emoji reactions (`emoji`, `parent` AT-URI) |
| `social.colibri.community` | Community records (`name`, `description`, `image`, `categoryOrder`) |
| `social.colibri.channel` | Channel records (`name`, `description`, `type`, `community` rkey, optional `category` rkey) |
| `social.colibri.category` | Category records (`name`, `emoji`, `community` rkey, optional `parent` rkey) |
| `social.colibri.membership` | Membership requests (`community` AT-URI) |
| `social.colibri.approval` | Owner approvals (`membership` AT-URI) |
| `app.bsky.actor.profile` | Bluesky profiles — watched for display name / avatar updates |

---

## Extending the event system

1. Add a new variant to `AppEvent` in `src/events.rs`.
2. Add a subscription field to `Subscriptions` in `src/ws_handler.rs` and handle it in `subscribe()`, `unsubscribe()`, and `matches()`.
3. Broadcast the event from wherever it originates (typically a handler in `src/jetstream.rs`).
