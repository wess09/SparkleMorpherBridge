# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project overview

SparkleMorpherBridge is a Folia server plugin (`folia-supported: true`, Minecraft 26.1.2, Java 25) that supplies the **server half of the `sparkle_morpher:2_6_0` Fabric plugin-message protocol** for Sparkle's Morpher. Players run the *unmodified* Fabric client; there is no companion client mod. The governing invariant behind nearly every design decision:

> Every byte the bridge sends must be indistinguishable from what an unmodified Sparkle's Morpher **Fabric server** would send to the stock client — but transported over Bukkit plugin messages, running under Folia with no main thread.

That means the on-the-wire codec, the XChaCha20 crypto, the handshake steps, the hash derivation, and the `server-cache` container format are all **fixed by the client** and must not drift. Source tree is split:

- `src/main/java/local/nanoda/sparklemorpher/` — this project's bridge code.
- `src/main/java/com/micaftic/morpher/` — **vendored, mostly-unchanged upstream** from Sparkle's Morpher (MIT) plus a Zstandard codec subtree (Apache-2.0). See `THIRD_PARTY_NOTICES.md` and `licenses/`. The bridge reaches into it through a narrow surface; see "Vendored format library" below.

## Building

A local Folia/"Lophine" API JAR is required to compile and is deliberately **not committed**. Point to it with the `foliaApiJar` Gradle property or `FOLIA_API_JAR` env var (runtime API jar, not the Paperclip bootstrap jar). Gradle 9.5.1 wrapper, Java 25 toolchain, Maven deps via Aliyun mirrors.

```powershell
./gradlew.bat clean assemble -PfoliaApiJar='C:\path\to\lophine-api.jar'
```

```bash
./gradlew clean assemble -PfoliaApiJar=/path/to/lophine-api.jar
```

The deployable artifact is `build/libs/SparkleMorpherBridge-<version>.jar` (built by the `fatJar` task; version lives in `build.gradle`, currently `0.3.2`). The fat JAR bundles the `implementation` deps (Gson, JOML, ImageStream); Netty, Paper API, Kyori Adventure, snakeyaml are `compileOnly` because the server provides them.

**There are no tests** (no `src/test`). Compile-only verification is `assemble` / `compileJava`; there is no lint task beyond the javac `-Xlint` flags in `build.gradle`. `main` class in `storage/BatchCompilerMain.java` is an offline tool that primes an existing server data directory (`java ... local.nanoda.sparklemorpher.storage.BatchCompilerMain <pluginDataFolder>`) and is the fastest way to exercise `ModelStore` + `ModelCompiler` without a server.

## Architecture

### Message flow and ownership

`SparkleMorpherBridgePlugin` (`local/nanoda/sparklemorpher/SparkleMorpherBridgePlugin.java`) is the **only class that touches Bukkit**. It registers the plugin channel, tracks negotiated players, and owns entity-tracking so it can implement "send to tracking and self" (its own `sendToTrackingAndSelf`) from the `PlayerTrackEntity`/`PlayerUntrackEntity` events instead of an NMS `PacketHandler`.

`onPluginMessageReceived` decodes the C2S frame via `SparkleWire.decodeClientMessage`, then routes each `ClientMessage` subtype to the correct executor:

- **`syncControl`** (2 threads): `LegacySyncService` handshake/pong parsing and small request parsing — latency sensitive, kept off the download pool.
- **`storageWorker`** (1 thread): *all* model disk I/O and compile work — upload chunk append, upload finish + compile.
- **Folia entity scheduler** (`player.getScheduler().run(...)`): anything that reads/writes player state or sends packets — selection, Molang, star, animation, upload-start/result replies, and the outbound drains.
- **`downloadWorker`** (fixed pool, `max(2, min(8, cores/2))`): streams model cache files and late pack entries.

These pools are all daemon threads created in `onEnable()` with comments explaining why each exists; uploads never stall a sync on a shared pool and handshakes never queue behind parked stream threads.

### Per-player entity-state sync (S2C-21)

The official server periodically pushes each player's flight/hunger state so remote
models animate correctly (the flight bit has no vanilla fallback on the client).
The bridge mirrors the parts that matter without NMS access: `SparkleWire`
`encodeModelState` embeds a flying/food snapshot into every S2C-4 model-state
frame, and each negotiated player runs a fixed-rate entity-scheduler task
(`startStateMonitor`/`tickPlayerState`) that seeds `knownFlying`/`knownFood` at
handshake and broadcasts narrow S2C-21 deltas (`encodePlayerStateFlying`/
`encodePlayerStateFood`) on change. Entity-state for *other* players is only ever
read on that player's own region (`queueModelState`), never cross-region. The
handshake also re-pushes the player's persisted star set as S2C-8
(`sendStars` → `ModelStore.stars`), matching the official join flow.

### Outbound flow control (the keepalive-kick fix)

The client's bounded sync queue will stall and get the player keepalive-kicked if the server floods it. So every S2C packet goes through a per-player `ConcurrentLinkedQueue<byte[]>` drain machine (`queuePayload` / `drainPayloads` / `clearPayloadQueue`) in the plugin: a one-at-a-time drain marker sends up to `download-chunks-per-tick` frames per tick, stops while the player's netty channel is not writable (`isChannelWritable`, resolved **reflectively** so the plugin compiles against paper-api only — degrades to `true` on renamed internals), and tracks queued-but-unsent bytes in `outboundPending`. `LegacySyncService` producers call `awaitBacklogCapacity(...)` to pause once a player's pending backlog exceeds `download-backlog-bytes`; streaming reads/encrypts one bounded chunk at a time and never holds a whole model in memory.

### Wire codec — `protocol/SparkleWire.java`

Pure static encode/decode with no Bukkit imports. `Reader`/`Writer` inner classes implement Minecraft-style VarInt/VarLong + big-endian primitives. Every read is length-capped (UTF strings, byte arrays, float maps/arrays all take a `maximumBytes`/count cap) and trailing data is rejected, because frames are attacker-controlled; malformed input raises `IllegalArgumentException` which the plugin logs at `FINE`. `ClientMessage` is a `sealed interface`; packet-id constants (`C2S_*`/`S2C_*`) are here. When adding a packet type you must update the sealed interface, the `decodeClientMessage` switch, and keep `PLUGIN_MESSAGE_LIMIT` (`1_048_576`, Bukkit `StandardMessenger` hard cap) in mind. Some S2C messages are bit-packed field selectors the client interprets as flags (e.g. `1 | 4096` full sync + Molang, `2048`/`4096` in `S2C_PLAYER_STATE`) — those bit values are part of the wire format.

### Legacy model-download sync — `protocol/LegacySyncService.java`

Server half of the unchanged client model-download protocol, a per-player `Session` state machine with a `step` field guarded by `synchronized (session)`:

1. Send garbage-header + type-1 handshake, encrypted with the shared `YsmCrypt.publicKey`, retaining the packet's `nextKey` as `session.key1`.
2. Client pong decrypts with `key1`; its **last 56 bytes are the manifest key** (`manifestKey`); type-2 confirmed.
3. Send a type-3 manifest listing models (folder-hash based identity the client uses to build its `server-cache` directory). It is a *single legacy frame*, so the whole manifest must fit the 1 MiB budget (`MANIFEST_PLAINTEXT_BUDGET`). Pack entries that don't fit with their icon (or at all) are trimmed/dropped from the manifest and re-pushed afterward as individual id-76 `S2C_PACK_ENTRY` frames on the download pool — newer clients upsert them so every pack and icon still appears.
4. The client replies with type-4 model requests that may be **split across several frames**; each parsed batch is queued on the session and one drain worker streams them in arrival order (parks briefly on empty for stragglers, bounded by `MAX_PENDING_REQUEST_HASHES`).
5. Each requested model is streamed as type-5 frames (garbage header + hashes + total + offset + length + plaintext chunk, encrypted with the rotating `key1`); type-6 is a transfer-failure frame.

Every `send` wraps encrypted bytes via `SparkleWire.encodeLegacySyncPayload` (discriminator 1 + data) and funnels into the plugin's outbound drain. The session checks `isCurrentSession` (session identity + player online) before every send so a superseded/reconnecting player never receives stale chunks. `remove(Player)` is called on quit and on a fresh handshake (see `SparkleMorpherBridgePlugin`).

### Persistence and intake — `storage/ModelStore.java`

Data folder layout (under `plugins/SparkleMorpherBridge`), each file with a load/save routine:

- `models/` — upload intake. Two shapes are scanned recursively (`models()`): single-file releases (`.ysm`/`.zip`/`.bbmodel`/`.gltf`/`.glb`) anywhere under the root, and **YSM folder libraries** — a directory carrying `ysm.json` (or `main.json` + `arm.json`) is treated as one opaque model and its subtree not re-scanned. A top-level child folder with `ysm-pack.json` (+ optional `ysm-pack.png`) is a "pack" exposed to the client's packs UI (`packs()`), parsed with Gson.
- `server-cache/` — generated files named `<16hex-hash1><16hex-hash2>`, the exact encrypted containers streamed to clients.
- `catalog.yml` — `CompiledModel` metadata keyed by canonical model id.
- `selections.yml`, `stars.yml` — per-player model selection and star set.
- `player-state.yml` — per-player Molang variable snapshots; **variable names are stored base64url-encoded as YAML keys** because raw names may be unsafe.
- `server-key.bin` — a random 56-byte server key (`loadOrCreateServerKey`). Do not copy it between servers or lose it: `hash1`/`hash2`, the cache filenames, and the whole cache identity derive from it.

Upload sessions are bounded and ordered: `begin` validates id/hash/size/permission (`sparklemorpher.upload`) and returns a status byte; `append` only accepts contiguous offsets; `finish` verifies the SHA-256 then atomically moves the file into place; `expireUploads` drops stale sessions. Buffers are allocated lazily (grown to bytes actually received, matching the official `ModelUploadSession`) so a begun-but-abandoned upload reserves no memory, and a player is capped at `MAX_UPLOADS_PER_PLAYER` concurrent sessions.

Per-player YAML persistence (`selections`/`stars`/`player-state`) is **written asynchronously and coalesced** onto a dedicated single-thread persist executor (`ModelStore.scheduleSave…`, created in the plugin as `persistWorker`), because the mutators run on Folia region threads which must never do blocking disk I/O. `flushPlayerStateSaves()` persists any pending snapshot on disable. (The catalog is written synchronously by `saveCatalog` on the storage worker, which is already off-region.)

Two distinct hashes are easy to confuse:
- `ModelFile.sha256` = hash of the raw **source** bytes (or sorted folder bytes) — the boot-skip identity.
- `CompiledModel.hash1/hash2` = derived from the model SHA + `serverKey` via `YsmCrypt.calculateModelHashes`; these are what the client requests and what names the cache file.

`publishCompiled` re-uses an existing valid cache file when the bytes/hashes match, else encrypts a fresh `server-cache` container. Boot rescan logic lives in the plugin's `compileStoredModels` + `isUpToDate`/`isKnownBad`: unchanged models skip re-decode, source files that failed once are remembered in-memory (`failedSources`, never persisted) so each boot retries a failing model at most once. Compilation runs on the storage worker; the catalog is not considered ready (`catalogReady`) until the scan's `finally`, and `LegacySyncService` **withholds the type-3 manifest** from a client that pongs before then (the client stays on its "preparing cache" stage instead of finishing against an empty catalog). When the scan finishes it calls `notifyCatalogReady()` to flush parked sessions; a successful upload re-syncs all connected clients (`resyncAllPlayers`) so the new model appears without a reconnect.

### Compilation — `storage/ModelCompiler.java`

Decodes a source model into a `RawYsmModel`, then re-serializes to the canonical binary the client cache wraps:

- `.ysm` → `YsmCrypt.decryptYsmFile` (XChaCha resource decryption) then `YSMBinaryDeserializer`.
- `.zip` or a model **folder** → `YSMFolderDeserializer` (JOML transforms, no whole-file read).
- Missing `modelHash` (`properties.sha256`) is a common legacy-export gap → falls back to a stable source-derived SHA-256.
- Serializes with `YSMBinarySerializer.serialize(rawModel, 32, true)` then `store.publishCompiled(...)`. `auth=false` for uploaded content.

### Vendored format library — `com/micaftic/morpher/`

Mirrors the YSM model binary/folder formats and the custom crypto/cache the unmodified client expects: XChaCha20/ChaCha20 with MT19937-derived key streams, CityHash-seeded key derivation, the Zstd codec in `core/zstd/`, `YsmCrypt` (encrypt/decrypt, `EncryptedPacket.nextKey` rotation, `encryptServerCache`/`decryptYsmFile`/`calculateModelHashes`), and a netty-backed `YSMByteBuf` writer whose "garbage header" (random prefix, `skipGarbageHeader`) is part of the frame format.

The bridge's integration surface is only: `core/security/YsmCrypt`, `core/security/YSMByteBuf`, `resource/YSMBinarySerializer`, `resource/YSMBinaryDeserializer`, `resource/YSMFolderDeserializer`, `resource/pojo/RawYsmModel`. Modify it only with extreme care — byte-exact format/crypto behavior is load-bearing — and preserve the upstream license headers. The plugin's `LocalModelScanner`/`ModelUploadSession`/`ModelResourceContainer` counterparts are here unused; the bridge re-implements server-side catalog/upload in `ModelStore`/`ModelCompiler`.

## Config

`config.yml` knobs, all re-read by `loadSettings()` and clamped to sane ranges there:
`allow-model-upload`, `max-model-bytes` (64 MiB default), `upload-chunk-bytes` (~30 KiB), `upload-chunks-per-tick`, `upload-timeout-seconds`, `download-chunks-per-tick`, `download-backlog-bytes`. `/sparklemorpher status|reload` command; `reload` calls `loadSettings()` and needs `sparklemorpher.admin`.

## Conventions and traps

- **Never emit a single plugin message over 1 MiB** — Bukkit refuses it (`StandardMessenger.validatePluginMessage`) and there is no chunked delivery path for the legacy sync frames; the manifest is budgeted *before* the pack count is written for exactly this reason.
- **No main thread.** Folia has none; any work touching a player must go through `player.getScheduler()`, and the plugin's own pools must stay off Bukkit API calls. `onDisable` shuts all pools down.
- Wire/crypto/format constants and bit masks are protocol, not policy — don't "clean up" values that look redundant.
- Malformed inbound frames are caught at `FINE` in the plugin; when debugging kicked/disconnected clients, look for the `[SM]` logger lines (in `LegacySyncService`) and the keepalive/flow-control comments — floods usually trace to backpressure, not protocol.
- Install/run notes and scope/license statements live in `README.md`; do not use Bukkit `/reload` in production.
- `.gitattributes` pins LF for sources; keep edits consistent with it.
