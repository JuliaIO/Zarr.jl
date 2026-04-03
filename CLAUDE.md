# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Zarr.jl is a Julia implementation of the Zarr specification for chunked, compressed, N-dimensional arrays. It supports Zarr v2 (stable) and v3 (experimental). The package provides multiple storage backends (filesystem, in-memory, S3, GCS, HTTP, ZIP) and compressors (Blosc, zlib, Zstandard).

## Build & Test Commands

```bash
# Run all tests
julia --project -e 'using Pkg; Pkg.test()'

# Run a single test file (use the test/ project environment)
julia --project=test -e 'using Test, Zarr, JSON; include("test/v3_codecs.jl")'

# Instantiate test dependencies (first time or after Julia version change)
julia --project=test -e 'using Pkg; Pkg.instantiate()'

# Run ZarrCore tests only
julia --project=lib/ZarrCore -e 'using Pkg; Pkg.test()'
```

Julia version requirement: 1.10+. CI tests against Julia LTS, stable (`1`), nightly, and pre-release on Ubuntu, macOS, and Windows.

### Test Structure

- `test/runtests.jl` — Main test suite: ZArray, groups, metadata, indexing, resize, concat, strings, ragged arrays, fill values. Includes the other test files.
- `test/storage.jl` — Storage backend tests
- `test/Filters.jl` — Filter tests
- `test/python.jl` — Interoperability tests with Python-generated Zarr files
- `test/v3_codecs.jl` — V3 codec tests (BytesCodec, TransposeCodec, etc.)
- `test/v3_julia.jl` — Julia-generated V3 test fixtures
- `test/v3_python.jl` — Python-generated V3 fixtures (requires PythonCall + CondaPkg; CI generates these before running tests)

## Architecture

### Package Structure
- `lib/ZarrCore/` — Minimal core package with types, registries, NoCompressor, BytesCodec, TransposeCodec, DirectoryStore, DictStore, ConsolidatedStore, all pure-Julia filters
- `src/` (Zarr.jl) — Wrapper that depends on ZarrCore, adds Blosc/Zlib/Zstd compressors, V3 compression codecs, HTTP/GCS/S3/ZIP stores
- ZarrCore's `default_compressor()` returns `NoCompressor()`; Zarr.jl overrides it to `BloscCompressor()`
- V3 codecs use `v3_codec_parsers` registry for extensible parsing; Zarr.jl registers compression codec parsers

### Core Type Hierarchy

```
AbstractMetadata{T,N,E<:AbstractChunkKeyEncoding}
├── MetadataV2{T,N,C,F}       # .zarray + .zattrs (v2)
└── MetadataV3{T,N,P,E}       # zarr.json (v3), P<:AbstractCodecPipeline, E<:AbstractChunkKeyEncoding

AbstractStore
├── DirectoryStore              # Filesystem
├── DictStore                   # In-memory Dict{String,Vector{UInt8}}
├── S3Store                     # AWS S3 (via AWSS3 extension)
├── GCStore                     # Google Cloud Storage
├── ConsolidatedStore           # Consolidated metadata wrapper
├── HTTPStore                   # HTTP-based read-only
└── ZipStore                    # ZIP archive read-only

AbstractCodecPipeline
├── V2Pipeline{C,F}            # compressor + filters
└── V3Pipeline{AA,AB,BB}       # three-phase codec chain

ZArray{T,N,S<:AbstractStore,M<:AbstractMetadata} <: AbstractDiskArray{T,N}
ZGroup{S<:AbstractStore}
```

### Module/File Layout

- `src/Zarr.jl` — Module entry point. Defines `ZarrFormat{V}` (Val-parameterized version tag) and default `DV = ZarrFormat(Val(2))`. Default chunk separator constants: `DS = '.'` (v2), `DS2 = '.'`, `DS3 = '/'`.
- `src/metadata.jl` — `MetadataV2` struct, type string encoding (`typestr`), fill value encoding/decoding, `Metadata()` constructors; dispatches V3 to `metadata3.jl`
- `src/metadata3.jl` — `MetadataV3{T,N,P,E}` struct and constructors, `Metadata3(dict)` parsing, `lower3` serialization, `get_order`, `JSON.lower(::MetadataV3)`. V3 type mappings use lowercase strings (e.g. `"float32"`, `"complex128"`)
- `src/chunkkeyencoding.jl` — `ChunkKeyEncoding` struct (separator char + prefix bool) and `SuffixChunkKeyEncoding{E}`, `citostring()` for chunk path generation, registry via `chunk_key_encoding_parsers` dict. V2 default: `'.'` separator, no prefix. V3 default: `'/'` separator, `"c/"` prefix
- `src/pipeline.jl` — `V2Pipeline` (wraps compressor + filters) and `V3Pipeline` (three-phase: array→array, array→bytes, bytes→bytes). `pipeline_encode`/`pipeline_decode!` interface
- `src/ZArray.jl` — Core array type, `readblock!`/`writeblock!` (DiskArrays interface), `zcreate`, `zzeros`, `zopen`, resize/append
- `src/ZGroup.jl` — Hierarchical group support, `zopen`, `zgroup`, auto-detection of zarr version via `ZarrFormat(store, path)`
- `src/Compressors/` — `Compressor` abstract type, `compressortypes` registry (keyed by spec name string), implementations: `blosc.jl`, `zlib.jl`, `zstd.jl`, `v3.jl` (v3 wrapper `Compressor_v3{C}`)
- `src/Codecs/` — V3 codec system (`Codec` abstract type), `V3/V3.jl` defines `V3Codec{In,Out}` with `BloscV3Codec`, `BytesCodec`, `CRC32cV3Codec`, `GzipV3Codec`, `ShardingCodec`, `TransposeCodec`, `ZstdV3Codec`. Registry: `codectypes` dict
- `src/Filters/` — `Filter{T,TENC}` abstract type, implementations for variable-length arrays, strings, Fletcher32, shuffle, delta, quantize
- `src/Storage/Storage.jl` — `AbstractStore` interface, I/O strategy (`SequentialRead`/`ConcurrentRead`), chunk read/write/delete helpers, metadata read/write dispatched on `ZarrFormat{2}` vs `ZarrFormat{3}`

### Key Design Patterns

- **Format version dispatch**: `ZarrFormat{V}` (where V is 2 or 3) used throughout for multiple dispatch on version-specific behavior (metadata file names, chunk encoding, serialization format)
- **Registry pattern**: Used pervasively — `compressortypes` (compressors), `codectypes` (V3 codecs), `chunk_key_encoding_parsers` (chunk key encodings), filter dict. New implementations register via these dicts.
- **Shape is mutable**: `metadata.shape` is `Base.RefValue{NTuple{N,Int}}` to allow `resize!` without replacing the metadata struct
- **Column-major ↔ row-major**: Zarr stores shapes/chunks in row-major (C order); Julia uses column-major. All conversions happen via `reverse()` at metadata boundaries
- **DiskArrays integration**: `ZArray <: AbstractDiskArray`, chunk-aware I/O via `readblock!`/`writeblock!`, `eachchunk`, `haschunks`
- **Async chunk I/O**: Channel-based parallel chunk reads/writes for stores supporting `ConcurrentRead`

### Storage Interface Requirements

New store backends must implement: `getindex(store, key)::Union{Vector{UInt8}, Nothing}`, `setindex!(store, data, key)`, `storagesize(store, path)`, `subdirs(store, path)`, `subkeys(store, path)`, `isinitialized(store, key)`, `storefromstring(Type, string, create)`.

### V3 Pipeline Architecture

V3 encoding/decoding follows a three-phase codec chain in `V3Pipeline`:

1. **array→array** codecs (e.g., `TransposeCodec`) — applied in forward order during encode
2. **array→bytes** codec (e.g., `BytesCodec`, `ShardingCodec`) — single codec, converts array to byte stream
3. **bytes→bytes** codecs (e.g., `GzipV3Codec`, `BloscV3Codec`, `CRC32cV3Codec`) — applied in forward order during encode

Decoding reverses all three phases. The `MetadataV3` convenience constructor builds the pipeline from `order` (→ `TransposeCodec`), `endian` (→ `BytesCodec`), and `compressor` (→ bytes→bytes codecs).

### V3 Status (Experimental)

- `MetadataV3{T,N,P,E}` has no `order` field; storage order is encoded in the pipeline via `TransposeCodec`. `get_order(md::MetadataV3)` derives `'C'`/`'F'` from the pipeline.
- Sharding codec (`sharding_indexed`) has struct definitions and encode/decode logic but throws `ArgumentError` when encountered (not wired into main pipeline)
- Filters are not implemented for V3 (`filters = nothing` hardcoded)
- `zgroup()` always creates v2 groups (hardcoded `DV` format)
- `crc32c` codec has full encode/decode implementations
- V3 groups work via `zarr.json` with `node_type: "group"`

### Extension System

S3 support is a weak dependency extension (`ext/ZarrAWSS3Ext.jl`), loaded only when `AWSS3` is imported. The extension registers a regex in `storageregexlist` for auto-detection of `s3://` URLs.
