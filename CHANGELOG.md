# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.1.10](https://github.com/colibri-social/appview/compare/v0.1.9...v0.1.10) - 2026-07-23

### Fixed

- Reactions ([#57](https://github.com/colibri-social/appview/pull/57))

## [0.1.9](https://github.com/colibri-social/appview/compare/v0.1.8...v0.1.9) - 2026-07-22

### Fixed

- Images/Attachments in FCM ([#56](https://github.com/colibri-social/appview/pull/56))
- Bump rust version to 1.96 ([#54](https://github.com/colibri-social/appview/pull/54))
- Don't show FCM notifications for currently viewed channel ([#53](https://github.com/colibri-social/appview/pull/53))

## [0.1.8](https://github.com/colibri-social/appview/compare/v0.1.7...v0.1.8) - 2026-07-22

### Fixed

- Deeplinking for FCM notifications ([#51](https://github.com/colibri-social/appview/pull/51))

## [0.1.7](https://github.com/colibri-social/appview/compare/v0.1.6...v0.1.7) - 2026-07-22

### Fixed

- Show user profile pictures and names in FCM notifications ([#49](https://github.com/colibri-social/appview/pull/49))

## [0.1.6](https://github.com/colibri-social/appview/compare/v0.1.5...v0.1.6) - 2026-07-22

### Added

- Collapsed FCM push notifications ([#47](https://github.com/colibri-social/appview/pull/47))

## [0.1.5](https://github.com/colibri-social/appview/compare/v0.1.4...v0.1.5) - 2026-07-22

### Added

- FCM notifications ([#45](https://github.com/colibri-social/appview/pull/45))

## [0.1.4](https://github.com/colibri-social/appview/compare/v0.1.3...v0.1.4) - 2026-07-21

### Added

- Push notifications for all message types ([#44](https://github.com/colibri-social/appview/pull/44))

### Fixed

- Backfill members from membership records ([#42](https://github.com/colibri-social/appview/pull/42))

## [0.1.3](https://github.com/colibri-social/appview/compare/v0.1.2...v0.1.3) - 2026-07-15

### Fixed

- Backfill members from membership records ([#40](https://github.com/colibri-social/appview/pull/40))
- Backfill members from membership records ([#39](https://github.com/colibri-social/appview/pull/39))

## [0.1.2](https://github.com/colibri-social/appview/compare/v0.1.1...v0.1.2) - 2026-07-15

### Added

- Better indexing for record data table ([#35](https://github.com/colibri-social/appview/pull/35))

### Fixed

- Thread channel scope through permission checks for mod stuff ([#34](https://github.com/colibri-social/appview/pull/34))
- Batch and cap listUnreadStatus queries, require membership ([#33](https://github.com/colibri-social/appview/pull/33))
- harden service auth checks ([#32](https://github.com/colibri-social/appview/pull/32))
- harden getImage endpoint ([#30](https://github.com/colibri-social/appview/pull/30))
- Make ?all=true param for listMessages endpoint mod only ([#31](https://github.com/colibri-social/appview/pull/31))
- Require member.ban permission to list banned members ([#29](https://github.com/colibri-social/appview/pull/29))
- Add pagination to remaining endpoints ([#28](https://github.com/colibri-social/appview/pull/28))
- Harden web-push implementation ([#27](https://github.com/colibri-social/appview/pull/27))
- Properly guard GET requests ([#26](https://github.com/colibri-social/appview/pull/26))
- enforce role hierarchy on endpoints ([#25](https://github.com/colibri-social/appview/pull/25))
- stop leaking mod-only events to all community clients ([#24](https://github.com/colibri-social/appview/pull/24))
- Stop disconnecting sockets on lag ([#23](https://github.com/colibri-social/appview/pull/23))
- Remove webhook call ([#21](https://github.com/colibri-social/appview/pull/21))

## [0.1.1](https://github.com/colibri-social/appview/compare/v0.1.0...v0.1.1) - 2026-07-13

### Fixed

- Update rust-ci.yml so runs don't duplicate ([#18](https://github.com/colibri-social/appview/pull/18))

### Other

- cache Rust builds in the release-plz workflow ([#20](https://github.com/colibri-social/appview/pull/20))

## [0.1.0](https://github.com/colibri-social/appview/releases/tag/v0.1.0) - 2026-07-12

### Added

- CI/CD release pipeline
- Mentionable roles
- voice channel mod tooling & permission
- VC
- Built-in SFU
- README with setup guide, example env file, Tap concurrency fix
- outbound humming manager plus declared communities for humming
- appview declaration for communities, adjustable web DID
- community migrations
- allowed roles & members on channel creation
- leave handler for community cleanup
- social.colibri.channel.getChannelView endpoint
- GIF picker
- custom colibri profiles + onboarding
- WebSocket auth via protocols
- performance improvements for backfill & events (sharding +
- More work
- Bot badges
- range blob handling
- More things(tm)
- Oh god so many things
- Credentials import script, missing events for roles and moderation
- delete endpoint for communities, full actor data for blocked users
- listApplications endpoint, member hydration for invitations
- Fix wrong reaction schema, wrong TID generation, memberships
- Image uploads for community creation workflow
- kickUser endpoint, read-time ban filtering, member-record revocation
- on-protocol moderation writes + community.create / registerCredentials
- community moderation, invitations, notifications, and cursor pagination
- Implement handling for most user messages
- typing events
- Tap event mapping
- multiple endpoints, initial event mapping & forwarding impl
- social.colibri.actor.listCommunities endpoint
- Working tap sync with record storage, social.colibri.actor.getData
- Authenticated WebSocket connection for sync endpoint
- DID document
- com.atproto.identity endpoints
- com.atproto.identity.resolveDid endpoint
- Rust workflows
- Add templates for issues and PRs
- better state handling for less websocket events, better backfill
- Owner only channels
- Endpoints for getting banned members, proper events, filtering of
- Community bans, channel reads
- New requires approval to join flag
- Voice channel announcements
- Set state via websocket
- Status fixes
- User status
- Handle bluesky profile updates via member_status_changed events
- Save handles and banner profiles, return status and emoji when
- Member statuses in member list
- User statuses
- Message blocking by community owner
- Add use endpoint to allow for limited codes
- Community lookup endpoint
- More infos for member events
- New endpoint for all invite codes for a community
- Attachments
- Backfill
- New `all` query param for messages endpoint
- Optimistic updates for channels and categories, sidebar endpoint
- Invitations & Basic blocking for non-joined users
- Message facets, new message endpoint
- Reaction data improvements
- Jetstream cursor resume & sweeps
- Emoji validation for reactions
- Reactions & better messages backfill
- convey message deletion, implement message editing
- Add atproto utils, backfill, account caching
- Add development dockerfile, cleanup

### Fixed

- Ensure releases on GitHub only get created once package is built
- Ensure docker-compose.yml uses pre-built image
- Swap out old ENV
- lint, failing flaky test
- Harden humming
- Authorization for humming & gated community streaming
- Spoofing prevention methods
- DID locking, handle resolution cache, collection pooling
- Hide spoilers in web push notifs
- Owner permissions, identification endpoint
- Slow indexing perf
- Eurosky relay, role permission inconsistencies
- Warnings & errors
- Proper state events
- Format & Clippy warnings
- Notifications, replies
- notification weirdness
- Some type issues
- Wrong channel handling
- Enum'd objects are nested
- Attempt to prevent empty profiles being stored
- Unban events
- Show communities for user if they allow pending members and user is
- Send member_left events when appropriate
- Send community deleted event to banned/kicked users
- Ensure non-approved members are marked as pending when a community
- Voice chat member logic
- Drop voice state if user is inactive for 5 Minutes (disconnect)
- Include current voice channel members in sidebar data
- Allow setting offline state
- Client status jitters
- Race condition
- Add new items to backfill
- Migrations
- Add missing subscriptions
- Allow empty JETSTREAM_URL env
- Approval and membership records
- Joined communities
- Memberships
- Emoji record indexing
- Update everything according to lexicon spec
- Add auth protection to invite creation endpoint
- Also send owner did when community is requested
- Better community fields, members endpoint
- Parse reactions correctly
- Backfill for emojis
- Update open-ssl usage

### Other

- Update rust-ci.yml
- Update sentry.rs
- Update docker-compose.yml
- Update mod.rs
- Update service_auth.rs
- Create get-community-password.sh
- Update docker-compose.yml
- Format & Lint
- Update service_auth.rs
- Update map_tap_event.rs
- Create channel_authz.rs
- Update docker-compose.yml
- Update docker-compose.yml
- fmt
- Update notifications.rs
- Update map_tap_event.rs
- Update colibri.rs
- Screw conventional commits, this was a lot of work
- Screw conventional commits, this was a lot of work
- prune now-redundant dead-code suppressions
- move `CommsBridge` + tap event loop into `lib::tap`
- centralize moderation write path through `moderation::issue_action`
- shared test fixtures in `lib::test_fixtures`
- split community/ into moderation/, invitations/, reads/ subdirs
- introduce `with_community_authz` / `with_authenticated` combinators
- standardize handler dependency injection on `&dyn Fn`
- satisfy `cargo fmt` and `cargo clippy -- -D warnings`
- Update docker-compose.yml
- fmt
- Lint fix
- Tests
- Add most event mappings
- Events prep
- Cleanup
- Update docker-compose.yml
- Update did.rs
- Auth for social.colibri.sync.subscribeEvents endpoint
- Update docker-compose.yml
- Update Dockerfile
- Update main.rs
- Update docker-compose.yml
- Update docker-compose.yml
- Update docker-compose.yml
- Update docker-compose.yml
- Update docker-compose.yml
- Update docker-compose.yml
- Update docker-compose.yml
- Update docker-compose.yml
- Update Dockerfile
- Update subscribe_events_handler.rs
- Add stub social.colibri.sync.subscribeEvents WebSocket endpoint
- Update mod.rs
- Update rust-ci.yml
- Update rust-ci.yml
- Update docker-compose.dev.yml
- initial implementation for com.atproto.repo.getRecord endpoint
- Merge pull request #6 from colibri-social/fix/action
- Update rust-ci.yml
- Update rust-ci.yml
- Attempt to get workflow to run
- Change YAML indentation
- Update main.rs
- Update rust.yml
- Update rust.yml
- Update rust.yml
- Update rust.yml
- SeaORM
- Update README.md
- Update sentry.rs
- Update main.rs
- Update rust.yml
- Update sentry.rs
- Update rust.yml
- Start work on rewrite
- Create LICENSE
- Update ws_handler.rs
- Update ws_handler.rs
- Update ws_handler.rs
- Update README.md
- Update ws_handler.rs
- Create 021_user_presence.sql
- Update db.rs
- Member updates are transmitted as part of a community
- Update 020_author_descriptions.sql
- Update 019_author_status.sql
- Update 019_author_status.sql
- Update 019_author_status.sql
- Update docker-compose.yml
- Update db.rs
- Update main.rs
- Update docker-compose.yml
- Update main.rs
- Update main.rs
- Update main.rs
- Update db.rs
- Update .gitignore
- Create 014_reset_community_backfill_picture.sql
- Update README.md
- Updated docs
- Create 011_reset_community_backfill.sql
- Update db.rs
- Update db.rs
- Create 010_community_extra_fields.sql
- Update db.rs
- Update docker-compose.yml
- Update jetstream.rs
- Update jetstream.rs
- Update jetstream.rs
- Update jetstream.rs
- stuff
- Update jetstream.rs
- Backfill logging, endpoints for reactions
- Update jetstream.rs
- Update db.rs
- Update ws_handler.rs
- Update ws_handler.rs
- Update main.rs
- Update main.rs
- Prepare for deployment
