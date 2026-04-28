# Changelog

## Unreleased

- Fast partial-read path for the `sharding_indexed` codec [#264](https://github.com/JuliaIO/Zarr.jl/pull/264)
  - in-memory partial decode in `Codecs.V3Codecs.read_shard_partial!` and `read_shard_partial_with_source!` — only inner chunks intersecting the requested slice are decompressed; the rest are skipped
  - storage-aware partial reads via three new optional `AbstractStore` methods (`supports_partial_reads`, `read_range`, `getsize`) — stores opt in to byte-range reads; safe defaults preserve correctness for backends that don't
  - `DirectoryStore` opts in (using `seek` + `readbytes!` and `filesize`); other backends inherit the defaults
  - new `Zarr.enable_partial_shard_storage_reads[]` `Ref{Bool}` flag (default `true`); flip to `false` to fall back to the in-memory partial-decode path for A/B comparisons
  - applies only when the codec pipeline is "pure" sharding (no array→array codecs before, no bytes→bytes codecs after); compound pipelines run on the existing path unchanged

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
