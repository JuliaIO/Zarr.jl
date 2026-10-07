# Changelog

This changelog covers every package in this repository. Version headings follow `Zarr.jl` releases.

## Unreleased

- Zarr v3 arrays can now store dimension names. Pass `dimension_names=("x", "y")` to `zcreate` and read them back with `dimension_names(z)`, so arrays written by Zarr.jl open with named dimensions in xarray. Give names in Julia's dimension order, and use `nothing` for an unnamed dimension. [#345](https://github.com/JuliaIO/Zarr.jl/pull/345), fixes [#319](https://github.com/JuliaIO/Zarr.jl/issues/319)
- `resize!` and `append!` now throw an error on read-only arrays instead of rewriting the stored metadata and deleting chunks. Resizing through a consolidated store now fails without changing the array, for both Zarr v2 and v3. [#342](https://github.com/JuliaIO/Zarr.jl/pull/342)
- Integer arrays using `DeltaFilter` now round-trip correctly when values wrap around the type's limits, such as `Int8[-128, 127, -128]`. [#336](https://github.com/JuliaIO/Zarr.jl/pull/336), fixes [#335](https://github.com/JuliaIO/Zarr.jl/issues/335)
- Zarr v2 arrays that use the `shuffle` or `fletcher32` filter with element types wider than one byte can now be read. Previously they could be written, but reading them threw a `BoundsError`. [#354](https://github.com/JuliaIO/Zarr.jl/pull/354)
- `ZarrHTTP` can now serve a `DirectoryStore` from its root path. It also rejects requests for files outside the served directory, including `..` paths and symlinks that point elsewhere, with a `400` response that explains why. [#344](https://github.com/JuliaIO/Zarr.jl/pull/344)
- `ZarrCore` is bumped to 0.11.1 to release the `ZarrCore` changes above. [#356](https://github.com/JuliaIO/Zarr.jl/pull/356)

## v0.11.0 - 2026-10-06

Zarr.jl is now split into several smaller packages. `using Zarr` still loads all of them, so most code needs no changes.

- The core array, group, metadata, and storage code now lives in the new lightweight `ZarrCore` package. [#317](https://github.com/JuliaIO/Zarr.jl/pull/317)
- The HTTP, GCS, S3, and ZIP storage backends moved into `ZarrHTTP`, `ZarrGCS`, `ZarrS3`, and `ZarrZip`. AWSS3 is still an optional dependency of `ZarrS3`. [#330](https://github.com/JuliaIO/Zarr.jl/pull/330)
- Blosc, zlib, and Zstandard support moved into `ZarrBlosc`, `ZarrZlib`, and `ZarrZstd`. Each package provides both the Zarr v2 compressor and the Zarr v3 codec. Blosc is the default compressor when `ZarrBlosc` is loaded; with `ZarrCore` alone, data is stored uncompressed by default. [#330](https://github.com/JuliaIO/Zarr.jl/pull/330)
- Compressors, codecs, and URL-based stores register themselves when their package loads. If several URL patterns match, the most specific one is used. To turn off automatic registration, set `RegisterAtInit = false` for `ZarrCore` in `LocalPreferences.toml` and call each package's `register!()` yourself. [#330](https://github.com/JuliaIO/Zarr.jl/pull/330)
- Zarr.jl now declares an explicit public API. Exported names are unchanged. Store, codec, filter, and compressor types, documented extension points, `missing_chunk_return_code!`, and `gcs_credentials` are public. Internal names such as `Metadata` and `ZarrFormat` are no longer available as `Zarr.x`; use `Zarr.ZarrCore.x` instead. `DateTime64`, `PermanentZarrCache`, `BloscCodec`, and `GzipCodec` are no longer accessible through `Zarr`. [#317](https://github.com/JuliaIO/Zarr.jl/pull/317), [#330](https://github.com/JuliaIO/Zarr.jl/pull/330)
- Fixed reading Zarr v3 arrays opened with `fill_as_missing=true`, which threw `cannot reinterpret UInt8 as Union{Missing,Float64}`. [#333](https://github.com/JuliaIO/Zarr.jl/pull/333)
- For contributors: each package now lives in its own top-level directory, and the root project is a Julia workspace. [#339](https://github.com/JuliaIO/Zarr.jl/pull/339)

## v0.10.2 - 2026-08-19

- Support HTTP.jl 2.x and drop 1.x [#322](https://github.com/JuliaIO/Zarr.jl/pull/322)
- Add logo and favicon to docs [#307](https://github.com/JuliaIO/Zarr.jl/pull/307)
- Support reading and writing variable-length strings [#311](https://github.com/JuliaIO/Zarr.jl/pull/311)
- Add more informative show method for S3Store [#320](https://github.com/JuliaIO/Zarr.jl/pull/320/)
- Fix complex fill values in python interop tests [#316](https://github.com/JuliaIO/Zarr.jl/pull/316)
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
