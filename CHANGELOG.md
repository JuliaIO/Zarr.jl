# Changelog

## Unreleased

- Added consolidated_metadata reading support for v3 [#287](https://github.com/JuliaIO/Zarr.jl/pull/287)
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
