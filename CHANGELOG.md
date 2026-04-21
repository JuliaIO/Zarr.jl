# Changelog

## Unreleased

### Added

- Enable `sharding_indexed` codec for Zarr v3: outer chunks (shards) are now split into inner chunks with a byte-range index, enabling efficient partial reads of large arrays
- Python zarr interoperability tests for sharded arrays using real fixtures generated via PythonCall/zarr-python
- Function-based codec registration system with typed `CodecEntry` for extensible V3 codec parsing
- `AbstractChunkKeyEncoding` and a chunk key encoding registration system

### Fixed

- `sharding_indexed` codec context propagation: inner codec parser now receives element-size context so codecs like `blosc` get the correct `typesize`
- `sharding_indexed` decode double-shift bug for `index_location=:start`: shard index offsets are now treated as absolute byte offsets from the shard start in all cases
- `sharding_indexed` decode fill value: `zdecode!` now accepts an optional `fill_value` argument so missing inner chunks are filled with the array fill value instead of `zero(T)`
- `pipeline_decode!` for `V2Pipeline` now accepts the `fill_value` keyword argument (ignored, but required to match the unified call site in `ZArray.readblock!`)
- Ragged inner chunks in `ShardingCodec` handled correctly