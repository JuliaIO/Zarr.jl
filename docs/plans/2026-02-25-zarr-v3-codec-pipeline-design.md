# Zarr V3 Codec Pipeline Design

Date: 2026-02-25
Branch: `fg/continue_v3pr`
Scope: Items 1-4 of v3 implementation gaps

## Goals

Full zarr-python v3 parity. Read-only sharding support (write sharding deferred). Public API (`zcreate`, `zopen`, `zzeros`, `zgroup`) stays stable. Internal v2 types (`MetadataV2`, `BloscCompressor`, all Filters) stay unchanged.

## 1. CodecPipeline Abstraction

New file: `src/pipeline.jl`

### Types

```julia
abstract type AbstractCodecPipeline end

struct V2Pipeline{C<:Compressor, F} <: AbstractCodecPipeline
    compressor::C
    filters::F  # Nothing or Tuple of Filters
end

struct V3Pipeline{AA, AB, BB} <: AbstractCodecPipeline
    array_array::AA   # Tuple of array->array codecs (e.g., TransposeCodec)
    array_bytes::AB   # Single array->bytes codec (e.g., BytesCodec or ShardingCodec)
    bytes_bytes::BB   # Tuple of bytes->bytes codecs (e.g., GzipCodec, CRC32cCodec)
end
```

The three-phase `V3Pipeline` mirrors the v3 spec's codec pipeline structure: optional array->array codecs, exactly one array->bytes codec, optional bytes->bytes codecs.

### Interface

```julia
pipeline_encode(p::AbstractCodecPipeline, data::AbstractArray) -> Union{Vector{UInt8}, Nothing}
pipeline_decode!(p::AbstractCodecPipeline, output::AbstractArray, compressed::Vector{UInt8})
```

`V2Pipeline` delegates to existing `zcompress!/zuncompress!` with zero behavior change.

`V3Pipeline` encode path: `foldl(array_array) |> array_bytes |> foldl(bytes_bytes)`. Decode is the reverse.

### Integration with ZArray

`compress_raw` and `uncompress_raw!` in `ZArray.jl` call the pipeline interface instead of directly calling `zcompress!/zuncompress!`. The pipeline is extracted from metadata:

```julia
get_pipeline(m::MetadataV2) = V2Pipeline(m.compressor, m.filters)
get_pipeline(m::MetadataV3) = m.pipeline
```

`readblock!` and `writeblock!` remain unchanged in structure — they call `compress_raw`/`uncompress_raw!` which now go through the pipeline.

## 2. MetadataV3 Redesign

### Struct Change

```julia
struct MetadataV3{T, N, P<:V3Pipeline} <: AbstractMetadata{T, N}
    zarr_format::Int
    node_type::String
    shape::Base.RefValue{NTuple{N, Int}}
    chunks::NTuple{N, Int}
    dtype::String
    fill_value::Union{T, Nothing}
    order::Char              # derived from transpose codec presence
    pipeline::P              # the full V3Pipeline
    chunk_encoding::ChunkEncoding
end
```

`compressor::C` and `filters::F` replaced by `pipeline::P`. Type parameters simplified to `{T, N, P}`.

### AbstractMetadata Change

`AbstractMetadata{T,N,C,F}` becomes `AbstractMetadata{T,N}`. The `C,F` parameters were v2-specific and not used by `ZArray`'s type parameter `M<:AbstractMetadata{T,N}`.

`MetadataV2{T,N,C,F}` keeps its existing type parameters for backward compatibility — it just adds `<: AbstractMetadata{T,N}` (dropping `C,F` from the supertype).

### Metadata3(dict) Parsing

Instead of extracting a single compressor and discarding the pipeline, `Metadata3(dict)` builds a `V3Pipeline` by categorizing each codec:

- `transpose` -> `array_array` tuple
- `bytes` -> `array_bytes`
- `sharding_indexed` -> `array_bytes`
- `blosc`, `gzip`, `zstd`, `crc32c` -> `bytes_bytes` tuple

### lower3 Serialization

Reconstructs the JSON codec chain from the pipeline's three phases in order.

## 3. V3 Codec Implementations

All codecs implement `codec_encode(codec, data)` and `codec_decode(codec, data)`.

### BytesCodec (array->bytes)
Reinterpret array as `Vector{UInt8}`. Little-endian only (matching current constraint).

### TransposeCodec (array->array)
Apply dimension permutation on encode, inverse on decode. Permutation stored in codec struct (parsed from metadata JSON `order` field).

### BloscCodec, GzipCodec, ZstdCodec (bytes->bytes)
Delegate to existing v2 `Compressor` implementations (`BloscCompressor`, `ZlibCompressor`, `ZstdCompressor`). The v3 codec is a typed wrapper that conforms to the `codec_encode`/`codec_decode` interface.

### CRC32cCodec (bytes->bytes)
Already has working `zencode!/zdecode!` in `V3/V3.jl`. Conform to `codec_encode`/`codec_decode` interface.

### ShardingCodec (array->bytes, read-only)

**Read path** (integrated into `pipeline_decode!`):

1. Parse shard index from start or end of byte buffer (based on `index_location`)
2. For each inner chunk in the shard:
   - Extract bytes using index offsets
   - Apply inner codec chain (ShardingCodec's nested `codecs`)
   - Place decoded inner chunk into correct position in output array
3. Empty inner chunks filled with fill_value

The existing `zdecode!` implementation in `V3/V3.jl` (line 463) handles this logic.

**Key insight**: From `readblock!`'s perspective, a shard IS a chunk. `readblock!` reads the shard file as raw bytes, passes them to `pipeline_decode!`, and the `ShardingCodec` handles all shard-internal logic transparently.

**Write path**: Not implemented in this iteration. Writing to sharded arrays will throw an error.

**Partial shard reads**: Full shard is decoded, then the relevant slice is extracted. This matches zarr-python behavior. Partial shard read optimization deferred.

## 4. V3 Group Creation

### Current Problem
`zgroup()` hardcodes v2 format: writes `.zgroup` file, ignores `zarr_format` parameter.

### Fix
Dispatch group metadata writing on `ZarrFormat`:

```julia
function write_group_metadata(::ZarrFormat{2}, s, path, attrs; indent_json=false)
    # Existing behavior: write .zgroup + .zattrs
end

function write_group_metadata(::ZarrFormat{3}, s, path, attrs; indent_json=false)
    # Write zarr.json with:
    # {"zarr_format": 3, "node_type": "group", "attributes": {...}}
end
```

`zgroup()` uses the existing `zarr_format` parameter (already in the signature) to dispatch.

## 5. Fix Test Fixtures

`test/v3_julia.jl` references removed `FormattedStore`. Replace with:

1. `DirectoryStore` directly
2. V3 root group via updated `zgroup(store, "", ZarrFormat(3))`
3. `zcreate(..., zarr_format=3)` for arrays (already in the API)

This depends on items 1-4 being implemented first.

## Dependency Order

```
1. AbstractMetadata{T,N} simplification
2. V3 codec type implementations (BytesCodec, TransposeCodec, etc.)
3. V3Pipeline struct + codec_encode/codec_decode
4. V2Pipeline struct (wrapping existing compressor+filters)
5. pipeline_encode/pipeline_decode! implementations
6. MetadataV3 redesign (store V3Pipeline, new Metadata3 parser)
7. ZArray.jl integration (compress_raw/uncompress_raw! use pipeline)
8. V3 group creation
9. Test fixture updates
10. Integration tests: round-trip Julia v3 write + read, read Python v3 fixtures
```
