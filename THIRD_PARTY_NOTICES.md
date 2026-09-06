# Third-party notices

This repository is licensed as a whole under AGPL-3.0-or-later, except where
an included file carries its own license. The notices below remain in force.

## Sparkle's Morpher

The YSM binary parser, serializer, crypto/cache helpers, model container types,
and selected utility classes under `src/main/java/com/micaftic/morpher/` are
derived from [Sparkle's Morpher](https://github.com/sdf123098/Sparkle-Morpher),
commit `cbbf7d93fb29bb2b09a72dd3e4fc52bfe7a4e3a9`, licensed under MIT.

The modified `YsmCrypt.java` in this repository adds the server-side model cache
identity fallback required by the bridge. Its upstream copyright notice is
preserved in `licenses/SPARKLE_MORPHER_MIT.txt`.

## Zstandard implementation

Files in `src/main/java/com/micaftic/morpher/core/zstd/` retain their Apache-2.0
headers. The Apache-2.0 text is in `licenses/APACHE-2.0.txt`.

## Yes Steve Model

This project implements the public YSM model transport and cache formats used by
the unmodified client. It does not include Yes Steve Model source files. The
format reference used during development is [YesSteveModel/YesSteveModel](https://github.com/YesSteveModel/YesSteveModel),
commit `fb25a93867066ee4f62c993ef47c1c62ce420560`, Apache-2.0.

## Runtime dependencies

Release JARs bundle Gson, JOML, and ImageStream. Their artifacts are resolved
from the repositories declared in `build.gradle`; downstream distributors must
retain their corresponding license notices.
