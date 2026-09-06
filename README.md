# SparkleMorpherBridge

An unofficial Folia server plugin that bridges the Sparkle's Morpher Fabric
client protocol to a Bukkit/Folia server. It targets Minecraft `26.1.2` and
Java 25.

This is not a replacement client mod. Players still install the unmodified
Sparkle's Morpher Fabric client. The plugin supplies the server half of the
existing `sparkle_morpher:2_6_0` plugin-message protocol.

## Features

- Server-side model uploads with size, timeout, and permission limits
- `.ysm` and YSM folder `.zip` release-package compilation
- Persistent `server-cache` generation compatible with the client cache flow
- Model selection, texture selection, actions, animation expressions, and
  Molang state forwarding to tracking players
- Folia-safe entity and region scheduling
- Reconnect-stable cache identity, so unchanged models are not re-downloaded
  on every login

## Installation

1. Build the project, or obtain a release JAR.
2. Place `SparkleMorpherBridge-<version>.jar` in the server `plugins` folder.
3. Restart the server. Do not use Bukkit `/reload`.
4. Edit `plugins/SparkleMorpherBridge/config.yml` if needed.

Uploaded source packages are kept under the plugin data folder. Generated
client-download entries are stored in its `server-cache` subdirectory.

## Commands and permissions

`/sparklemorpher status` shows negotiated clients and compiled-model count.

`/sparklemorpher reload` reloads the bridge configuration and requires
`sparklemorpher.admin`.

`sparklemorpher.upload` controls in-game model uploads. It is granted by
default; revoke it through LuckPerms if uploads should be staff-only.

## Building

A local Lophine/Folia API JAR is required for compilation because it provides
the Folia API. It is deliberately not committed to this repository. Use the
runtime API JAR, not the Paperclip bootstrap JAR.

```powershell
./gradlew.bat clean assemble -PfoliaApiJar='C:\path\to\lophine-api.jar'
```

```bash
./gradlew clean assemble -PfoliaApiJar=/path/to/lophine-api.jar
```

The deployable artifact is `build/libs/SparkleMorpherBridge-0.3.2.jar`.

## Scope

The bridge only covers the model protocol paths implemented in this repository.
It does not render models server-side, distribute client mods, or grant model
asset licenses. Model creators remain responsible for the files they upload.

## License

The bridge code is available under [AGPL-3.0-or-later](LICENSE). Included and
runtime third-party components are documented in
[THIRD_PARTY_NOTICES.md](THIRD_PARTY_NOTICES.md).
