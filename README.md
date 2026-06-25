# appview

The app view that sits behind [colibri.social](https://colibri.social).

## Development

- ENVs (SENTRY_DSN)
- Min rust version
- Start command

### Web Push (VAPID)

Background push notifications (delivered to the web client's service worker when
the app is closed) require a VAPID keypair. Generate one with:

```sh
npx web-push generate-vapid-keys
```

Then set these env vars (see `.env`):

| Var                 | Description                                                            |
| ------------------- | ---------------------------------------------------------------------- |
| `VAPID_PUBLIC_KEY`  | Base64url public key. **Must match** the website's `PUBLIC_VAPID_KEY`. |
| `VAPID_PRIVATE_KEY` | Base64url private key. Keep secret.                                    |
| `VAPID_SUBJECT`     | Contact URI for the push service, e.g. `mailto:pds@colibri.social`.    |

When these are unset, background Web Push is disabled (the AppView still emits
the live WebSocket `notification_event`); the boot log says which mode is active.

### Building on Windows

Web Push pulls in OpenSSL (via the transitive `ece` crate). On Linux/Docker the
system `libssl` is used (the Dockerfile installs `libssl-dev`). Windows has no
system OpenSSL to link against, so `Cargo.toml` builds it from source there
(`openssl` with the `vendored` feature, Windows-only). That build needs two
tools on your `PATH`:

- **Strawberry Perl**: <https://strawberryperl.com/>
- **NASM**: <https://www.nasm.us/> (the installer does _not_ add itself to `PATH`; add its install folder manually)

With [Chocolatey](https://chocolatey.org/install): `choco install strawberryperl nasm -y`.

Open a fresh terminal afterwards so the updated `PATH` is picked up
(`perl --version` / `nasm --version` to confirm), then `cargo run`. The first
build compiles OpenSSL from source (a few minutes); it's cached afterwards and
statically linked, so there's no `OPENSSL_DIR` to set and no runtime DLLs.

If the OpenSSL build fails with `Can't locate Locale/Maketext/Simple.pm`, the
wrong Perl is being used. Most often Git Bash's bundled MSYS Perl
(`C:\Program Files\Git\usr\bin\perl.exe`), which lacks modules OpenSSL's
`Configure` needs and shadows Strawberry Perl on `PATH`. Point the OpenSSL build
at Strawberry Perl explicitly (works in any shell; add it to your `~/.bashrc` to
make it stick):

```sh
export OPENSSL_SRC_PERL="C:/Strawberry/perl/bin/perl.exe"
```

## Tap

For configuring Tap, see the [Tap README](https://github.com/bluesky-social/indigo/blob/main/cmd/tap/README.md).
