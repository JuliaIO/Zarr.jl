# ZarrCore.jl Extraction Design

## Goal

Factor Zarr.jl into two packages:
- **ZarrCore.jl** (`lib/ZarrCore/`) — minimal core sufficient to read/write uncompressed Zarr v2 and v3 arrays
- **Zarr.jl** — depends on ZarrCore, adds all compressors, V3 codecs, and network/archive storage backends

## Design Decisions

1. **Abstract pipeline in Core, concrete codecs split across packages.** ZarrCore defines `AbstractCodecPipeline`, `V2Pipeline`, `V3Pipeline`, and only `BytesCodec` + `TransposeCodec`. Zarr.jl registers compression codecs into the existing `codectypes` registry. Unknown codecs at parse time produce a clear error.

2. **All pure-Julia filters in Core.** FixedScaleOffset, Fletcher32, Shuffle, Delta, Quantize — all tiny, no external deps. Avoids complex split of the Filters directory.

3. **Core storage: DirectoryStore + DictStore + ConsolidatedStore.** No external deps. ConsolidatedStore is a thin JSON wrapper commonly used for read-only access.

4. **Monorepo subdirectory package** at `lib/ZarrCore/`. Own `Project.toml`, publishable to General registry. Zarr.jl uses `path = "lib/ZarrCore"` during dev.

5. **Zarr.jl re-exports everything from ZarrCore.** `using Zarr` is fully backward-compatible. `using ZarrCore` gives the minimal API.

6. **Convenience functions in Core.** `zcreate`, `zopen`, `zzeros`, `zgroup` live in ZarrCore — without them Core has no user-facing functionality.

7. **Extensible store resolution.** ZarrCore owns `zopen` dispatch through `storageregexlist`. Core handles local paths + DictStore. Zarr.jl registers HTTP/GCS/S3/Zip resolvers.

## Package Structure

### lib/ZarrCore/

```
lib/ZarrCore/
  Project.toml          # JSON, DiskArrays, OffsetArrays, DateTimes64, Dates
  src/
    ZarrCore.jl         # module entry, ZarrFormat, constants, includes, exports
    chunkkeyencoding.jl
    metadata.jl
    metadata3.jl
    MaxLengthStrings.jl
    pipeline.jl
    Compressors/
      Compressors.jl    # Compressor, NoCompressor, compressortypes registry
    Codecs/
      Codecs.jl         # abstract Codec, zencode/zdecode
      V3/
        V3.jl           # BytesCodec, TransposeCodec only
    Filters/
      Filters.jl        # Filter + all pure-Julia filter implementations
      fixedscaleoffset.jl, fletcher32.jl, shuffle.jl, delta.jl, quantize.jl, vlenfilters.jl
    Storage/
      Storage.jl        # AbstractStore, storageregexlist, metadata I/O
      directorystore.jl
      dictstore.jl
      consolidated.jl
    ZArray.jl
    ZGroup.jl
```

### Zarr.jl (top-level, rewritten as thin wrapper)

```
src/
  Zarr.jl              # using ZarrCore, re-exports, registrations
  Compressors/
    blosc.jl
    zlib.jl
    zstd.jl
  Codecs/
    V3/
      blosc.jl
      gzip.jl
      zstd.jl
      crc32c.jl
      sharding.jl
  Storage/
    http.jl
    gcstore.jl
    zipstore.jl
```

## Registration Pattern

Zarr.jl's `__init__` populates ZarrCore's registries:

```julia
# Compressors
ZarrCore.compressortypes["blosc"] = BloscCompressor
ZarrCore.compressortypes["zlib"]  = ZlibCompressor
ZarrCore.compressortypes["zstd"]  = ZstdCompressor

# V3 codecs
ZarrCore.codectypes["blosc"]  = BloscV3Codec
ZarrCore.codectypes["gzip"]   = GzipV3Codec
ZarrCore.codectypes["zstd"]   = ZstdV3Codec
ZarrCore.codectypes["crc32c"] = CRC32cV3Codec

# Store URL resolvers
push!(ZarrCore.storageregexlist, r"^https?://" => HTTPStore)
push!(ZarrCore.storageregexlist, r"^gs://"     => GCStore)
```

## Migration Strategy

- **Move** files from `src/` to `lib/ZarrCore/src/` (preserves git history)
- **Split** `Compressors/Compressors.jl`: abstract type + NoCompressor + registry to Core; blosc/zlib/zstd stay
- **Split** `Codecs/V3/V3.jl`: BytesCodec + TransposeCodec to Core; compression codecs stay
- **Split** `Storage/`: directorystore + dictstore + consolidated to Core; http/gcstore/zipstore stay
- **Rewrite** top-level `src/Zarr.jl` as thin wrapper

## ZarrCore Dependencies

```toml
[deps]
JSON = "..."
DiskArrays = "..."
OffsetArrays = "..."
DateTimes64 = "..."
Dates = "..."        # stdlib
```

## Zarr.jl Dependencies (in addition to ZarrCore)

```toml
[deps]
ZarrCore = "..."
Blosc = "..."
CRC32c = "..."
ChunkCodecCore = "..."
ChunkCodecLibZlib = "..."
ChunkCodecLibZstd = "..."
HTTP = "..."
OpenSSL = "..."
URIs = "..."
ZipArchives = "..."
```
