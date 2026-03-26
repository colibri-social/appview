# colibri-appview

> [!WARNING]
> This implementation is almost entirely vibe-coded for the purpose of being able to quickly get started with development of the main application. It will be re-written in the near future to take advantage of [Tap](https://github.com/bluesky-social/indigo/blob/main/cmd/tap/README.md) and be reworked to include all user data storage as well as any OAuth capabilities, which currently reside within the website's backend. If you are interested in helping with this, start a [discussion](https://github.com/colibri-social/appview/discussions) on this repo!

An [ATProto](https://atproto.com/) appview for the **Colibri** social platform, built with [Rocket.rs](https://rocket.rs/).

## Overview

| Feature                  | Details                                                                                                                                                                                                                                                                     |
| ------------------------ | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **Jetstream consumer**   | Connects to a Colibri Jetstream WebSocket and ingests all Colibri lexicon records into PostgreSQL in real time, with cursor persistence and automatic reconnect.                                                                                                            |
| **Backfill**             | On startup, fetches historical records from each known DID's PDS so no data is missed. Triggered again automatically when the appview falls behind. Backfill includes Bluesky profiles (`banner_url`, `handle`, `description`) and actor status/emoji.                      |
| **Client subscriptions** | Clients connect via WebSocket (`/api/subscribe?did=<did>`) and subscribe to filtered event streams — messages by channel, community events by AT-URI. Community subscriptions automatically deliver member status and profile updates. Easy to extend with new event types. |
| **Presence**             | Clients are marked online on WS connect, offline on disconnect, and away after 5 minutes of heartbeat-only activity. Preferred state (`online`/`away`/`dnd`) is persisted and restored on reconnect.                                                                        |
| **WebRTC signaling**     | Room-based peer-to-peer voice/video signaling over WebSocket (`/api/webrtc/signal`).                                                                                                                                                                                        |
| **REST API**             | Full set of read endpoints for messages, authors, reactions, communities, channels, categories, members, and invite codes.                                                                                                                                                  |

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

| Variable         | Required | Default                                             | Description                                                                                                                                          |
| ---------------- | -------- | --------------------------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------- |
| `DATABASE_URL`   | ✅       | —                                                   | PostgreSQL connection string, e.g. `postgres://user:pass@localhost:5432/colibri`                                                                     |
| `INVITE_API_KEY` | ✅       | —                                                   | Bearer token required for `POST /api/invite`, `DELETE /api/invite/<code>`, `GET /api/invites`, `POST /api/message/block`, and `POST /api/user/state` |
| `ROCKET_ADDRESS` | ❌       | `0.0.0.0`                                           | Bind address                                                                                                                                         |
| `ROCKET_PORT`    | ❌       | `8000`                                              | Bind port                                                                                                                                            |
| `JETSTREAM_URL`  | ❌       | `wss://jetstream2.us-east.bsky.network/subscribe?…` | Override Jetstream endpoint (e.g. point at a self-hosted instance)                                                                                   |
| `RUST_LOG`       | ❌       | `colibri_appview=info,rocket=info`                  | Log filter (`trace`, `debug`, `info`, `warn`, `error`)                                                                                               |

---

## REST API

All endpoints return JSON. CORS is open (`*`).

### Voice presence

- Each DID can only be active in one voice channel. Joining a different channel triggers a `voice_channel_updated` event for the previous room before the new join is emitted, so clients see the previous membership cleared.
- If a connection sends no non-heartbeat action for more than five minutes, it is marked away and the DID is removed from any active voice call so voice membership stays in sync.

The Jetstream consumer enforces the community's `requiresApprovalToJoin` flag when indexing messages. Communities with `requiresApprovalToJoin = true` (default) only accept messages from approved members and the owner; those that set the flag to `false` allow any DID with a membership declaration to send messages even before approval. If a community flips from open to approval-only while the appview is running, any member lacking an approval record is reverted to `pending` and a `member_pending` event is emitted so clients can refresh their UI.

### Messages

#### `GET /api/messages`

Paginated message history for a channel, newest first. Each message includes the author profile.

| Parameter | Required | Description                                                               |
| --------- | -------- | ------------------------------------------------------------------------- |
| `channel` | ✅       | Channel rkey                                                              |
| `limit`   | ❌       | 1–100, default 50                                                         |
| `before`  | ❌       | ISO 8601 timestamp — returns messages older than this (pagination cursor) |
| `all`     | ❌       | Fetch all messages from the channel                                       |

```bash
curl "http://localhost:8000/api/messages?channel=general&limit=20"
curl "http://localhost:8000/api/messages?channel=general&before=2024-03-01T12:00:00Z"
```

#### `GET /api/message`

Fetch a single message by author and record key.

| Parameter | Required | Description |
| --------- | -------- | ----------- |
| `author`  | ✅       | Author DID  |
| `rkey`    | ✅       | Record key  |

```bash
curl "http://localhost:8000/api/message?author=did:plc:xxx&rkey=3mxxx"
```

#### `POST /api/message/block` 🔒

Block a message, hiding it from all future responses. All connected WebSocket clients are immediately notified via a `message_deleted` event. Requires `Authorization: Bearer <INVITE_API_KEY>`.

Returns `204 No Content` on success, `404` if the message doesn't exist or is already blocked.

| Parameter    | Required | Description        |
| ------------ | -------- | ------------------ |
| `author_did` | ✅       | Author DID         |
| `rkey`       | ✅       | Message record key |

```bash
curl -X POST "http://localhost:8000/api/message/block?author_did=did:plc:xxx&rkey=3mxxx" \
  -H "Authorization: Bearer $INVITE_API_KEY"
```

### Authors

#### `GET /api/authors`

Retrieve a cached author profile. Falls back to a live ATProto fetch if not yet cached.

| Parameter | Required | Description |
| --------- | -------- | ----------- |
| `did`     | ✅       | Author DID  |

```bash
curl "http://localhost:8000/api/authors?did=did:plc:xxx"
```

**Response:**

```json
{
	"did": "did:plc:xxx",
	"display_name": "Alice",
	"avatar_url": "https://cdn.bsky.app/...",
	"banner_url": "https://cdn.bsky.app/...",
	"description": "Building cool things",
	"handle": "alice.bsky.social",
	"status": "Working on something cool",
	"emoji": "🚀",
	"state": "online"
}
```

### Reactions

#### `GET /api/reactions`

Reactions for a single message, grouped by emoji with reactor DIDs.

| Parameter | Required | Description         |
| --------- | -------- | ------------------- |
| `message` | ✅       | Target message rkey |

#### `GET /api/reactions/channel`

All reactions in a channel, keyed by target message rkey.

| Parameter | Required | Description  |
| --------- | -------- | ------------ |
| `channel` | ✅       | Channel rkey |

**Response:**

```json
{
	"3mxxx": [{ "emoji": "👍", "count": 3, "reactor_dids": ["did:plc:..."] }],
	"3myyy": [{ "emoji": "❤️", "count": 1, "reactor_dids": ["did:plc:..."] }]
}
```

### Communities

#### `GET /api/community`

Look up a single cached community by AT-URI or rkey.

| Parameter   | Required | Description                      |
| ----------- | -------- | -------------------------------- |
| `community` | ✅       | AT-URI (`at://...`) or bare rkey |

```bash
curl "http://localhost:8000/api/community?community=at://did:plc:xxx/social.colibri.community/3mxxx"
curl "http://localhost:8000/api/community?community=3mxxx"
```

The response now includes a `requires_approval_to_join` boolean, which mirrors the BlueSky `requiresApprovalToJoin` flag. When `false`, the appview will accept messages from DIDs that merely have a membership declaration; when `true`, only approved members (and the owner) are allowed to post in that community.

Returns `404` if the community is not cached.

### Community bans

Use the protected invite API key to ban or unban members from posting or reacting within a community. Banned DIDs are silently dropped by the Jetstream consumer — their messages and reactions are never indexed or broadcast.

When a user is banned, a `member_left` event is broadcast to all community members so clients can update their UI in real-time.

When a user is unbanned:
- If they still have a membership declaration AND the community doesn't require approval records (`requiresApprovalToJoin = false`), a `member_joined` event is broadcast (they can immediately participate again)
- Otherwise, no event is sent (they must wait for approval in approval-required communities, or rejoin if they left)

Banned members are also excluded from:
- `/api/members` responses (community member lists)
- `/api/communities` responses (user won't see communities they're banned from)

**Endpoints:**

- `POST /api/community/ban` (body `{ "community_uri": "...", "member_did": "did:..." }`) — adds the DID to the ban list and broadcasts a `member_left` event. Returns `204 No Content`.
- `DELETE /api/community/ban?community=<uri>&member_did=<did>` — removes the ban entry. If the user still has a membership and the community is open, broadcasts a `member_joined` event. Returns `204 No Content` on success, `404` if the DID was not banned.
- `GET /api/community/bans?community=<uri>` — returns array of banned members with full profile data (same structure as `/api/members`), sorted by most recently banned first.

All ban endpoints require `Authorization: Bearer <INVITE_API_KEY>`.

**Example ban list response:**

```json
[
  {
    "member_did": "did:plc:banned1",
    "status": "banned",
    "display_name": "Banned User",
    "avatar_url": "https://...",
    "banner_url": "https://...",
    "description": "User bio",
    "handle": "banned.bsky.social",
    "status_text": "Away",
    "emoji": "😞",
    "state": "offline"
  }
]
```

#### `GET /api/communities`

All communities for a user — both owned and joined — in a single roundtrip.

Includes:
- Communities owned by the user
- Communities where the user is an approved member
- Communities where the user is pending BUT the community has `requiresApprovalToJoin = false` (open communities where pending members can participate)

**Excludes:**
- Communities where the user is banned

| Parameter | Required | Description |
| --------- | -------- | ----------- |
| `did`     | ✅       | User DID    |

**Response:**

```json
{
  "owned": [
    {
      "owner_did": "did:plc:xxx",
      "rkey": "3mxxx",
      "name": "My Community",
      "description": "...",
      "picture": { "$type": "blob", "ref": { ... }, "mimeType": "image/jpeg", "size": 12345 },
      "category_order": [ "3mcat1", "3mcat2" ]
    }
  ],
  "joined": [ ... ]
}
```

#### `GET /api/channels`

All channels for a community.

| Parameter   | Required | Description                                                         |
| ----------- | -------- | ------------------------------------------------------------------- |
| `community` | ✅       | Community AT-URI (`at://did:plc:xxx/social.colibri.community/rkey`) |

**Response:** array of channel objects with `uri`, `rkey`, `name`, `description`, `channel_type`, `category_rkey`, `community_uri`, and `voice_members`.

`voice_members` is the sorted list of DIDs currently connected to that channel's voice room (`[]` if empty).

#### `GET /api/sidebar`

Channels and categories combined into a sidebar-ready structure. Categories nest their channels; channels with no category appear under `uncategorized`.

| Parameter   | Required | Description      |
| ----------- | -------- | ---------------- |
| `community` | ✅       | Community AT-URI |

**Response:**

```json
{
	"categories": [
		{
			"uri": "at://...",
			"rkey": "3mxxx",
			"name": "General",
			"channel_order": ["3mch1", "3mch2"],
			"channels": [
				{
					"uri": "...",
					"rkey": "3mch1",
					"name": "announcements",
					"channel_type": "text",
					"category_rkey": "3mxxx",
					"voice_members": []
				},
				{
					"uri": "...",
					"rkey": "3mch2",
					"name": "general",
					"channel_type": "text",
					"category_rkey": "3mxxx",
					"voice_members": ["did:plc:abc"]
				}
			]
		}
	],
	"uncategorized": [
		{
			"uri": "...",
			"rkey": "...",
			"name": "off-topic",
			"channel_type": "text",
			"category_rkey": null,
			"voice_members": []
		}
	]
}
```

#### `GET /api/members`

All members of a community, enriched with cached profile data. The owner is always included with `status: "owner"`. 

**Note:** Banned members are excluded from this list.

| Parameter   | Required | Description      |
| ----------- | -------- | ---------------- |
| `community` | ✅       | Community AT-URI |

**Response:**

```json
[
	{
		"member_did": "did:plc:xxx",
		"status": "owner",
		"display_name": "Alice",
		"avatar_url": "https://cdn.bsky.app/...",
		"banner_url": "https://cdn.bsky.app/...",
		"description": "Building cool things",
		"handle": "alice.bsky.social",
		"status_text": "Working on something cool",
		"emoji": "🚀",
		"state": "online"
	},
	{
		"member_did": "did:plc:yyy",
		"status": "approved",
		"display_name": "Bob",
		"avatar_url": null,
		"banner_url": null,
		"description": null,
		"handle": "bob.bsky.social",
		"status_text": null,
		"emoji": null,
		"state": "away"
	},
	{
		"member_did": "did:plc:zzz",
		"status": "pending",
		"display_name": null,
		"avatar_url": null,
		"banner_url": null,
		"description": null,
		"handle": null,
		"status_text": null,
		"emoji": null,
		"state": null
	}
]
```

> **Note:** `status` is the membership role (`owner`/`approved`/`pending`). `status_text` is the user's custom status message from `social.colibri.actor.data`. `state` is the presence state (`online`/`away`/`dnd`/`offline`).

### Invite codes

#### `GET /api/invites` 🔒

Return all invite codes for a community, newest first. Requires `Authorization: Bearer <INVITE_API_KEY>`.

| Parameter   | Required | Description      |
| ----------- | -------- | ---------------- |
| `community` | ✅       | Community AT-URI |

**Response:**

```json
[
	{
		"code": "abc123",
		"community_uri": "at://...",
		"created_by_did": "did:plc:xxx",
		"max_uses": 10,
		"use_count": 3,
		"active": true
	}
]
```

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

#### `POST /api/invite/<code>/use` 🔒

Mark an invite code as used. Increments `use_count` and enforces `max_uses` — if the code is inactive or exhausted, returns `410 Gone`. Requires `Authorization: Bearer <INVITE_API_KEY>`.

Returns `204 No Content` on success, `410 Gone` if the code is inactive or has reached `max_uses`.

### Presence

#### `POST /api/user/state` 🔒

Manually set a user's presence state. Updates both the current state and the **preferred state** — the preferred state is restored automatically when the user reconnects via WebSocket. Requires `Authorization: Bearer <INVITE_API_KEY>`.

Valid states: `online`, `away`, `dnd`. `offline` is managed automatically by the WebSocket connection.

| Parameter | Required | Description                |
| --------- | -------- | -------------------------- |
| `did`     | ✅       | User DID                   |
| `state`   | ✅       | `online`, `away`, or `dnd` |

```bash
curl -X POST "http://localhost:8000/api/user/state?did=did:plc:xxx&state=dnd" \
  -H "Authorization: Bearer $INVITE_API_KEY"
```

Returns `204 No Content` on success, `422 Unprocessable Entity` for an invalid state value.

### Blobs

#### `GET /api/blob`

Proxy a blob from the author's PDS. Resolves the DID to its PDS endpoint and forwards the request, relaying `Range`, `Content-Type`, `Content-Range`, `Content-Length`, and `Accept-Ranges` headers transparently. Returns `206 Partial Content` when a `Range` header is sent.

| Parameter | Required | Description                                            |
| --------- | -------- | ------------------------------------------------------ |
| `did`     | ✅       | Author DID                                             |
| `cid`     | ✅       | Blob CID — the `$link` value from the ATProto blob ref |

```bash
curl "http://localhost:8000/api/blob?did=did:plc:xxx&cid=bafkrei..."
# Partial content / streaming
curl -H "Range: bytes=0-1023" "http://localhost:8000/api/blob?did=did:plc:xxx&cid=bafkrei..."
```

Returns `404` if the DID cannot be resolved, `502` if the PDS request fails.

### Channel reads

#### `GET /api/channel-reads`

Retrieve all channel read cursors for a DID. These records are synced from the user's PDS via Jetstream, reflecting `social.colibri.channel.read` events.

Each read cursor indicates the last timestamp the user marked a channel as read. Clients can compare this cursor to message timestamps to determine which channels have unread messages.

| Parameter | Required | Description |
| --------- | -------- | ----------- |
| `did`     | ✅       | User DID    |

**Response:**

```json
[
  {
    "channel_uri": "at://did:plc:xxx/social.colibri.channel/3mxxx",
    "cursor_at": "2024-03-15T14:30:00Z"
  },
  {
    "channel_uri": "at://did:plc:yyy/social.colibri.channel/3myyy",
    "cursor_at": "2024-03-15T12:00:00Z"
  }
]
```

The `cursor_at` timestamp is parsed from the `cursor` field in the ATProto record. Results are sorted newest first.

---

## WebSocket: real-time events — `GET /api/subscribe`

Connect with any WebSocket client. Pass `?did=<did>` to enable presence tracking for a user.

```
ws://localhost:8000/api/subscribe
ws://localhost:8000/api/subscribe?did=did:plc:xxx
```

When `did` is provided:

- On connect: restores the user's preferred state (default `online`) and broadcasts a `user_status_changed` event
- On disconnect: sets state to `offline` and broadcasts
- After 5 minutes of heartbeat-only messages: sets state to `away` and broadcasts
- On next non-heartbeat message while away: restores preferred state and broadcasts

Send JSON subscription requests; receive JSON events.

### Subscribing

```json
// Messages in a specific channel
{ "action": "subscribe", "event_type": "message", "channel": "<channel-rkey>" }

// Messages in all channels
{ "action": "subscribe", "event_type": "message" }

// All events for a specific community (channels, categories, members, community metadata,
// plus status/profile updates for all community members automatically)
{ "action": "subscribe", "event_type": "community", "community_uri": "at://did:plc:xxx/social.colibri.community/<rkey>" }

// Status/profile updates for a specific user (also covered by community subscription)
{ "action": "subscribe", "event_type": "user_status", "did": "did:plc:xxx" }

// Status/profile updates for all users
{ "action": "subscribe", "event_type": "user_status" }

// Unsubscribing works the same way
{ "action": "unsubscribe", "event_type": "message", "channel": "<channel-rkey>" }
{ "action": "unsubscribe", "event_type": "community", "community_uri": "at://..." }
{ "action": "unsubscribe", "event_type": "user_status", "did": "did:plc:xxx" }

// Keepalive — does NOT reset the away timer
{ "action": "heartbeat" }

// Signal user activity (e.g. after sending a message via REST) — resets away timer
{ "action": "activity" }

// Voice presence updates — send when the user joins or leaves a voice channel
{ "action": "voice_event", "community_uri": "<community-uri>", "voice_channel_rkey": "<record-key>", "voice_action": "join" }
{ "action": "voice_event", "community_uri": "<community-uri>", "voice_channel_rkey": "<record-key>", "voice_action": "leave" }

// Set presence state — updates both current state and preferred state (restored on reconnect)
{ "action": "set_state", "state": "online" }
{ "action": "set_state", "state": "away" }
{ "action": "set_state", "state": "dnd" }
{ "action": "set_state", "state": "offline" }
```

Valid states for `set_state`: `online`, `away`, `dnd`, `offline`.

Multiple subscriptions are cumulative. You can subscribe to several channels and/or several communities at once.

> **Community subscriptions** automatically include `user_status_changed` and `user_profile_updated` events for all current and future members of that community — no separate `user_status` subscription needed.

### Events — `message` subscription

Delivered to clients subscribed to the matching channel.

#### `message`

A new message was posted (or an existing one updated). Includes full author profile embedded directly in the message object.

```json
{
	"type": "message",
	"message": {
		"id": "uuid",
		"rkey": "3mxxx",
		"author_did": "did:plc:xxx",
		"display_name": "Alice",
		"avatar_url": "https://cdn.bsky.app/...",
		"banner_url": "https://cdn.bsky.app/...",
		"description": "Building cool things",
		"handle": "alice.bsky.social",
		"status_text": "Working on something",
		"emoji": "🚀",
		"state": "online",
		"text": "Hello!",
		"channel": "general",
		"created_at": "2024-03-01T12:00:00Z",
		"indexed_at": "2024-03-01T12:00:01Z"
	},
	"parent": null
}
```

#### `message_deleted`

```json
{
	"type": "message_deleted",
	"id": "uuid",
	"rkey": "3mxxx",
	"author_did": "did:plc:xxx",
	"channel": "general"
}
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

> **Presence & profile updates are automatic.** When you subscribe to a community, you also receive `user_status_changed` and `user_profile_updated` events for all current and future members of that community — including the owner. No separate `user_status` subscription is needed. When a new member joins, they are added to the watch-list live; when a member leaves, they are removed.

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
  "picture": { "$type": "blob", "ref": { ... }, "mimeType": "image/jpeg", "size": 12345 },
  "category_order": ["3mcat1", "3mcat2"]
}
```

#### `community_deleted`

Sent when:
- The community owner deletes their community record (all members receive this)
- A user's membership record is removed from a community (only that user receives this as a "you were removed" notification)

```json
{
	"type": "community_deleted",
	"community_uri": "at://...",
	"owner_did": "did:plc:xxx",
	"rkey": "3mxxx"
}
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
	"description": "Important announcements",
	"channel_type": "text",
	"category_rkey": "3mcat"
}
```

#### `channel_deleted`

```json
{
	"type": "channel_deleted",
	"community_uri": "at://...",
	"uri": "at://...",
	"rkey": "3mxxx"
}
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
	"channel_order": ["3mch1", "3mch2"]
}
```

#### `category_deleted`

```json
{
	"type": "category_deleted",
	"community_uri": "at://...",
	"uri": "at://...",
	"rkey": "3mxxx"
}
```

#### `member_pending`

Sent when:
- A user requests to join (membership record created, awaiting approval)
- A user's approval record is deleted in a community that does NOT require approval (demotion, user can still chat)

```json
{
	"type": "member_pending",
	"community_uri": "at://...",
	"member_did": "did:plc:yyy",
	"membership_uri": "at://..."
}
```

#### `member_joined`

A user was approved and is now a full member.

```json
{
	"type": "member_joined",
	"community_uri": "at://...",
	"member_did": "did:plc:yyy",
	"membership_uri": "at://..."
}
```

#### `member_left`

Sent when:
- A user's membership record is deleted (user left or was removed)
- A user's approval record is deleted in a community that requires approval (user was kicked/blocked and can no longer participate)
- A user is banned from a community (via `POST /api/community/ban`)

Includes full member profile data (same fields as `/api/members`):

```json
{
	"type": "member_left",
	"community_uri": "at://...",
	"member_did": "did:plc:yyy",
	"display_name": "User Name",
	"avatar_url": "https://...",
	"banner_url": "https://...",
	"description": "User bio",
	"handle": "user.bsky.social",
	"status_text": "Custom status",
	"emoji": "👋",
	"state": "offline"
}
```

#### `voice_channel_updated`

Emitted whenever the active member list of a voice channel changes (join, leave, disconnect, or `set_state` to `offline`).

```json
{
	"type": "voice_channel_updated",
	"community_uri": "at://did:plc:xxx/social.colibri.community/3mxxx",
	"channel_rkey": "3mvoice1",
	"member_dids": ["did:plc:abc", "did:plc:xyz"]
}
```

`member_dids` is the full sorted list of DIDs currently marked as present in that voice channel. When the list becomes empty clients receive the event with `[]`, which should clear any UI state.

---

### Events — `user_status` subscription

Delivered to clients with a `user_status` subscription matching the DID (or all users if no DID filter). Also automatically delivered to clients with a **community** subscription that includes the user as a member or owner — no separate subscription needed in that case.

#### `user_status_changed`

A user updated or deleted their `social.colibri.actor.data` record, or their presence state changed (connect/disconnect/away).

```json
{
	"type": "user_status_changed",
	"did": "did:plc:xxx",
	"status": "Working on something cool",
	"emoji": "🚀",
	"state": "online",
	"display_name": "Alice",
	"avatar_url": "https://..."
}
```

| Field          | Always present | Description                                               |
| -------------- | -------------- | --------------------------------------------------------- |
| `did`          | ✅             | User DID                                                  |
| `status`       | ✅             | Status text (max 32 chars); empty string if deleted       |
| `emoji`        | ❌             | Optional status emoji                                     |
| `state`        | ❌             | Presence state: `online`, `away`, `dnd`, or `offline`     |
| `display_name` | ❌             | Cached display name (omitted if not yet in profile cache) |
| `avatar_url`   | ❌             | Cached avatar URL                                         |

#### `user_profile_updated`

A user updated their Bluesky profile (`app.bsky.actor.profile`). Only fired for users already in the profile cache.

```json
{
	"type": "user_profile_updated",
	"did": "did:plc:xxx",
	"display_name": "Alice",
	"avatar_url": "https://cdn.bsky.app/...",
	"banner_url": "https://cdn.bsky.app/...",
	"description": "Building cool things",
	"handle": "alice.bsky.social"
}
```

All fields except `did` are optional — only present if the profile record contained them.

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

| Collection                  | Description                                                                                                                                         |
| --------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------- |
| `social.colibri.message`    | Chat messages (`text`, `channel`, `createdAt`, optional `parent`, `facets`)                                                                         |
| `social.colibri.reaction`   | Emoji reactions (`emoji`, `targetMessage` record-key)                                                                                               |
| `social.colibri.community`  | Community records (`name`, `description`, `picture` blob, `categoryOrder`)                                                                          |
| `social.colibri.channel`    | Channel records (`name`, `description`, `type`, `community` rkey, `category` rkey)                                                                  |
| `social.colibri.category`   | Category records (`name`, `channelOrder`, `community` rkey)                                                                                         |
| `social.colibri.membership` | Membership requests (`community` AT-URI)                                                                                                            |
| `social.colibri.approval`   | Owner approvals (`membership` AT-URI, `community` AT-URI)                                                                                           |
| `social.colibri.actor.data` | User status and emoji (`status` text max 32 chars, optional `emoji`). Upserted to profile cache; triggers `user_status_changed`.                    |
| `app.bsky.actor.profile`    | Bluesky profiles — watched for display name, avatar, banner, description updates. Resolved directly from the commit payload (no AppView roundtrip). |

---

## Extending the event system

1. Add a new variant to `AppEvent` in `src/events.rs`.
2. Add a subscription field to `Subscriptions` in `src/ws_handler.rs` and handle it in `subscribe()`, `unsubscribe()`, and `matches()`.
3. Broadcast the event from wherever it originates (typically a handler in `src/jetstream.rs`).
