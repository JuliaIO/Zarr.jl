# Changelog

## Unreleased
- Support HTTP.jl 2.x and drop 1.x
- Move code to ZarrCore.jl with low dependencies
- Split the zip backend out into a `ZarrZip` subpackage (`lib/ZarrZip`), so `ZarrCore` no longer depends on ZipArchives.jl. `Zarr` hard-depends on `ZarrZip` and re-exports it, so `Zarr.ZipStore` and `Zarr.writezip` are unchanged; the `Zarr` facade now re-exports the API of every subpackage in `Zarr.REEXPORTED_MODULES`
- Split the three compressors out into `ZarrBlosc` (`lib/ZarrBlosc`), `ZarrZlib` (`lib/ZarrZlib`) and `ZarrZstd` (`lib/ZarrZstd`), so `ZarrCore` no longer depends on Blosc.jl, ChunkCodecCore.jl, ChunkCodecLibZlib.jl or ChunkCodecLibZstd.jl. Each package holds both the zarr v2 compressor and the matching zarr v3 codec (`BloscCompressor`/`BloscV3Codec`, `ZlibCompressor`/`GzipV3Codec`, `ZstdCompressor`/`ZstdV3Codec`) and registers them at load time. `Zarr` hard-depends on all three and re-exports them, so `Zarr.BloscCompressor` & co. are unchanged. `Zarr` also sets the default compressor to `BloscCompressor()` in its `__init__`; a bare `ZarrCore` now defaults to `NoCompressor()`
- Split the two network backends out into `ZarrHTTP` (`lib/ZarrHTTP`) and `ZarrGCS` (`lib/ZarrGCS`), so `ZarrCore` no longer depends on HTTP.jl or URIs.jl and is down to seven dependencies. `ZarrHTTP` holds `HTTPStore` and the `HTTP.serve`/`HTTP.serve!` methods that serve a store/array/group; `ZarrGCS` holds `GCStore` and `gcs_credentials`. Both register their URL patterns into `ZarrCore.storageregexlist` at load time, and that registry now ranks patterns by specificity rather than registration order, so `https://storage.googleapis.com/...` still resolves to `GCStore` whichever package loads first. `Zarr` hard-depends on both and re-exports them, so `Zarr.HTTPStore`, `GCStore` and `zopen("gs://...")` are unchanged
- Split the S3 backend out into `ZarrS3` (`lib/ZarrS3`). AWSS3.jl stays a *weak* dependency, but moves down from the `Zarr` umbrella to `ZarrS3`, which now declares the `ZarrS3AWSS3Ext` extension; `Zarr` itself no longer has a `[weakdeps]`/`[extensions]` section. `ZarrS3` owns the `S3Store` type, the "load AWSS3" fallback constructor and the `s3://` registration, and `Zarr` hard-depends on it, so `Zarr.S3Store` resolves and `zopen("s3://...")` gives the same friendly error as before without pulling AWSS3 (~330 ms of load time) into every session. With this, `ZarrCore.storageregexlist` starts out empty: every URL-addressable backend now lives in a subpackage and registers itself at load time
- `missing_chunk_return_code!` and `gcs_credentials` are now public API (`Zarr.missing_chunk_return_code!`)
- Public names are now declared in a per-package `public_names_*.jl` of bare `public` statements, included under `@static if VERSION >= v"1.11"`; the `ZarrCore.@public` macro and the `PUBLIC_NAMES` registries are gone. `Zarr` reads those files directly on Julia 1.10, where `public` does not exist, so the API surface is identical on LTS and on 1.11+. `Zarr`'s own public list (`src/public_names_zarr.jl`) is the user-facing subset; the full extension API stays public on the package that defines it
- `Zarr.DateTime64` and `Zarr.PermanentZarrCache` are no longer public. Depend on DateTimes64.jl directly for `DateTime64`
- Removed the unused `BloscCodec` and `GzipCodec` v3 codec names; the registered codecs are `BloscV3Codec` (ZarrBlosc) and `GzipV3Codec` (ZarrZlib)
- Declare an explicit public API [#317](https://github.com/JuliaIO/Zarr.jl/pull/317). Every store, codec, filter and compressor type, and every documented extension point, is now `public`; the set of exported names is unchanged. Internals (`Metadata`, `ZarrFormat`, `is_zarray`, `is_zgroup`, `normalize_path`, `MaxLengthString`, ...) are no longer reachable as `Zarr.x` and must be accessed via `Zarr.ZarrCore.x`
- Add logo and favicon to docs [#307](https://github.com/JuliaIO/Zarr.jl/pull/307)
- Support reading and writing variable-length strings [#311](https://github.com/JuliaIO/Zarr.jl/pull/311)
- Remove lru keyword from zopen, should use DiskArrays.cache instead

## v0.10.1 - 2026-07-07

- Add caching to menu, update sharding description [#306](https://github.com/JuliaIO/Zarr.jl/pull/306)
- Added support for the Zarr V3 `fixed_length_utf32` string data type [#305](https://github.com/JuliaIO/Zarr.jl/pull/305)
- Added consolidated_metadata reading and writing support for v3 [#287](https://github.com/JuliaIO/Zarr.jl/pull/287)
- Fix the `ConcurrentRead` write path silently dropping chunk writes; affected stores such as `S3Store` wrote array metadata but no data chunks [#297](https://github.com/JuliaIO/Zarr.jl/pull/297)
- Added reading compat for stores produced in python with `numcodecs.blosc` [#286](https://github.com/JuliaIO/Zarr.jl/pull/286)
- Bump HTTP compat to 2 [#284](https://github.com/JuliaIO/Zarr.jl/pull/284)
- V2 performance improvements [#280](https://github.com/JuliaIO/Zarr.jl/pull/280)
  - `readblock!` fast path for single-chunk full-reads that bypasses the readtask channel and the chunk-shaped scratch buffer, decoding straight into the output array
  - `writeblock!` fast path for single-chunk full-overwrites that bypasses the readtask/writetask channels and the chunk-shaped scratch buffer, encoding straight from the input array into the store
  - Zero-copy chunk write for `NoCompressor` + no filters: the single-chunk fastpath hands a `reinterpret(UInt8, ain)` view straight to the store, skipping the chunk-sized `Vector{UInt8}` allocation + memcpy
  - `NoCompressor` writes: replace `append!` over the reinterpret view with bulk `resize!` + `copyto!` in the generic `zcompress!` fallback
  - `NoCompressor` reads: bulk-copy `zuncompress!` method dispatched on `::NoCompressor` bypasses `copyto!(::Array, ::ReinterpretArray)`'s element-by-element walk
  - `getchunkarray_undef` skips the dead zero-fill of the chunk-shaped scratch buffer on full-overwrite paths
- Added manual pagination in order to go beyond the default 1k [#282](https://github.com/JuliaIO/Zarr.jl/pull/282)
- Added `wait` to writetask in `writeblock!` [#281](https://github.com/JuliaIO/Zarr.jl/pull/281)
- Fix getattrs for v3 [#277](https://github.com/JuliaIO/Zarr.jl/pull/277)
- Add `CachingStore`, a store that caches reads from a remote store in a local cache store [#231](https://github.com/JuliaIO/Zarr.jl/pull/231)
- Fix CondaPkg branch in CI, use release version instead [#273](https://github.com/JuliaIO/Zarr.jl/pull/273)
- Fix creation of on-disk arrays that do not fit in memory [#269](https://github.com/JuliaIO/Zarr.jl/pull/269)
- Add another consistent caching approach through `zarrcache` [#299](https://github.com/JuliaIO/Zarr.jl/pull/293)

## v0.10.0 - 2026-04-24

- Enable `sharding_indexed` codec for Zarr v3 [#241](https://github.com/JuliaIO/Zarr.jl/pull/241)
  - outer chunks (shards) are now split into inner chunks with a byte-range index, enabling efficient partial reads of large arrays
  - Python zarr interoperability tests for sharded arrays using real fixtures generated via PythonCall/zarr-python
  - Function-based codec registration system with typed `CodecEntry` for extensible V3 codec parsing
  - `sharding_indexed` codec context propagation: inner codec parser now receives element-size context so codecs like `blosc` get the correct `typesize`
  - `sharding_indexed` decode double-shift bug for `index_location=:start`: shard index offsets are now treated as absolute byte offsets from the shard start in all cases
  - `sharding_indexed` decode fill value: `zdecode!` now accepts an optional `fill_value` argument so missing inner chunks are filled with the array fill value instead of `zero(T)`
  - `pipeline_decode!` for `V2Pipeline` now accepts the `fill_value` keyword argument (ignored, but required to match the unified call site in `ZArray.readblock!`)
  - Ragged inner chunks in `ShardingCodec` handled correctly
- fixes UI str parsing [#259](https://github.com/JuliaIO/Zarr.jl/pull/259)
- added zarr_format to ZGroup [#258](https://github.com/JuliaIO/Zarr.jl/pull/258)
- Add support for S3Path and remove deprecated global_aws_config() [#253](https://github.com/JuliaIO/Zarr.jl/pull/253)
- (docs) get started [#246](https://github.com/JuliaIO/Zarr.jl/pull/246)
- update badges [#245](https://github.com/JuliaIO/Zarr.jl/pull/245)
- setup vitepress [#243](https://github.com/JuliaIO/Zarr.jl/pull/243)
- Registration system for chunk key encoding [#242](https://github.com/JuliaIO/Zarr.jl/pull/242)
- test: Consolidate CondaPkg.add calls [#240](https://github.com/JuliaIO/Zarr.jl/pull/240)
- test: Read Julia-generated v3 fixtures with Python zarr [#239](https://github.com/JuliaIO/Zarr.jl/pull/239)
- Use CondaPkg.jl#206 for nightly, fix manifest_uuid_path [#238](https://github.com/JuliaIO/Zarr.jl/pull/238)
- fix docs build [#237](https://github.com/JuliaIO/Zarr.jl/pull/237)
- consolidate methods [#235](https://github.com/JuliaIO/Zarr.jl/pull/235)
- Add AbstractChunkKeyEncoding [#234](https://github.com/JuliaIO/Zarr.jl/pull/234)
- V3 codec pipeline, group creation, and Python interop [#232](https://github.com/JuliaIO/Zarr.jl/pull/232)
- Add read support for big endian [#230](https://github.com/JuliaIO/Zarr.jl/pull/230)
- Indent method/type signatures on docstrings [#229](https://github.com/JuliaIO/Zarr.jl/pull/229)
- Promote header levels in tutorial.md [#228](https://github.com/JuliaIO/Zarr.jl/pull/228)
- Continue on v3 PR [#226](https://github.com/JuliaIO/Zarr.jl/pull/226)
- move AWSS3 into extension [#224](https://github.com/JuliaIO/Zarr.jl/pull/224)
