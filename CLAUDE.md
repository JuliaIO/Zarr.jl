# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Zarr.jl is a Julia implementation of the Zarr specification for chunked, compressed, N-dimensional arrays. It supports Zarr v2 (stable) and v3 (experimental, in development on `as/continue_v3` branch). The package provides multiple storage backends (filesystem, in-memory, S3, GCS, HTTP, ZIP) and compressors (Blosc, zlib, Zstandard).

## Build & Test Commands

```bash
# Run all tests
julia --project -e 'using Pkg; Pkg.test()'

# Run a single test file interactively (use the test/ project environment)
julia --project=test -e 'using Test, Zarr, JSON; include("test/v3_codecs.jl")'

# Instantiate test dependencies (after Julia version change or first time setup)
julia --project=test -e 'using Pkg; Pkg.develop(path=pwd()); Pkg.resolve(); Pkg.instantiate()'

# Generate the v3 test fixtures (required before running the suite; CI does this
# in a separate step, `runtests.jl` does not do it for you)
julia --project=test test/v3_julia.jl
julia --project=test test/v3_python.jl
```

Julia version requirement: 1.10+. CI tests against Julia LTS, stable (`1`), nightly, and pre-release on Ubuntu, macOS, and Windows.

Because 1.10 is still supported, the `public` keyword (Julia 1.11+) cannot be used directly. Use the `ZarrCore.@public` macro instead — it expands to `public` on 1.11+ and to nothing on 1.10.

## Architecture

### Core Type Hierarchy

```
AbstractMetadata{T,N}
├── MetadataV2{T,N,C,F}   # .zarray + .zattrs (v2)
└── MetadataV3{T,N,P}     # zarr.json (v3), P<:AbstractCodecPipeline

AbstractStore
├── DirectoryStore          # Filesystem
├── DictStore               # In-memory Dict{String,Vector{UInt8}}
├── S3Store                 # AWS S3 (via AWSS3 extension)
├── GCStore                 # Google Cloud Storage
├── ConsolidatedStore       # Consolidated metadata wrapper
├── HTTPStore               # HTTP-based read-only
└── ZipStore                # ZIP archive read-only

ZArray{T,N,S<:AbstractStore,M<:AbstractMetadata} <: AbstractDiskArray{T,N}
ZGroup{S<:AbstractStore}
```

### Package Layout

The repo is a Pkg workspace with two packages:

- `ZarrCore` (`lib/ZarrCore/`) — the entire implementation. Every type, method and docstring lives here.
- `Zarr` (`src/Zarr.jl`) — a thin facade that re-exports `ZarrCore`'s API. It mirrors the export/public split: names `ZarrCore` exports are re-exported, names it only marks `@public` stay public (not exported). Nothing else is forwarded, so internals must be reached as `Zarr.ZarrCore.foo`.

### Public API Policy

Set by [a maintainer comment on PR #317](https://github.com/JuliaIO/Zarr.jl/pull/317#issuecomment-5314176722). Anything a downstream consumer could need, plus every documented extension point, is part of the public API; everything else is internal and may change.

- **Exported** (in scope after `using Zarr`): `ZArray`, `ZGroup`, `zopen`, `zzeros`, `zcreate`, `zgroup`, `zarrcache`, `storagesize`, `storageratio`, `zinfo`, `DirectoryStore`, `S3Store`, `GCStore`.
- **Public but not exported**: every store, codec, filter and compressor type; the store/filter/compressor/codec/chunk-key-encoding extension interfaces; `typestr`, `fill_value_encoding`, `fill_value_decoding`, `zname`, `writezip`, `consolidate_metadata`, `DateTime64`.
- **Internal**: `Metadata`/`MetadataV2`/`MetadataV3`, `ZarrFormat`, `DV`, `is_zarray`, `is_zgroup`, `normalize_path`, `MaxLengthString`, `ASCIIChar`, `ShapeOnlyArray` (should be removed), `getattrs`/`writeattrs`/`getmetadata`/`writemetadata`, `V2Pipeline`/`V3Pipeline`/`pipeline_encode`/`pipeline_decode!`, and the `store_*` helpers.

Tests follow the same rule: public names are used as `Zarr.foo`, internals as `ZarrCore.foo` (each test file does `import Zarr: ZarrCore`). If a test needs `ZarrCore.` for something a downstream user would plausibly need, that is a signal the name should be made public rather than the test qualified.

### Module/File Layout

All paths below are relative to `lib/ZarrCore/`.

- `src/ZarrCore.jl` — Module entry point, defines `ZarrFormat{V}` (Val-parameterized version tag, default `DV = ZarrFormat(Val(2))`), the `@public` macro, and the export/public declarations
- `src/metadata.jl` — `MetadataV2` struct, type string encoding (`typestr`), fill value encoding/decoding, `Metadata()` constructors for V2; dispatches V3 to `metadata3.jl`
- `src/metadata3.jl` — All V3-specific code: `MetadataV3` struct and constructors, `Metadata3(dict)` parsing, `lower3` serialization, codec pipeline parsing, `get_order`, `JSON.lower(::MetadataV3)`
- `src/chunkkeyencoding.jl` — `ChunkKeyEncoding` struct (separator char + prefix bool), `citostring()` for chunk path generation, plus the `register_chunk_key_encoding` registry. V2 default: `'.'` separator, no prefix. V3 default: `'/'` separator, `"c/"` prefix
- `src/ZArray.jl` — Core array type, `readblock!`/`writeblock!` (DiskArrays interface), `zcreate`, `zzeros`, `zopen`, resize/append
- `src/ZGroup.jl` — Hierarchical group support, `zopen`, `zgroup`, auto-detection of zarr version via `ZarrFormat(store, path)`
- `src/Compressors/` — `Compressor` abstract type, `compressortypes` registry (keyed by spec name string), implementations: `blosc.jl`, `zlib.jl`, `zstd.jl`
- `src/Codecs/` — V3 codec system (`Codec` abstract type), `V3/V3.jl` defines `V3Codec{In,Out}` with `BloscV3Codec`, `BytesCodec`, `CRC32cV3Codec`, `GzipV3Codec`, `ShardingCodec`, `TransposeCodec`, `ZstdV3Codec`, and the `register_codec` registry
- `src/Filters/` — `Filter{T,TENC}` abstract type, implementations for variable-length arrays, strings, Fletcher32, shuffle, delta, quantize
- `src/Storage/Storage.jl` — `AbstractStore` interface, I/O strategy (`SequentialRead`/`ConcurrentRead`), chunk read/write/delete helpers, metadata read/write dispatched on `ZarrFormat{2}` vs `ZarrFormat{3}`

### Key Design Patterns

- **Format version dispatch**: `ZarrFormat{V}` (where V is 2 or 3) used throughout for multiple dispatch on version-specific behavior (metadata file names, chunk encoding, serialization format)
- **Shape is mutable**: `metadata.shape` is `Base.RefValue{NTuple{N,Int}}` to allow `resize!` without replacing the metadata struct
- **Column-major ↔ row-major**: Zarr stores shapes/chunks in row-major (C order); Julia uses column-major. All conversions happen via `reverse()` at metadata boundaries
- **Compressor registry**: `compressortypes` dict maps spec names (e.g., `"blosc"`, `"zlib"`) to compressor types. V3 uses `Compressor_v3{C}` wrapper to change JSON serialization format
- **DiskArrays integration**: `ZArray <: AbstractDiskArray`, chunk-aware I/O via `readblock!`/`writeblock!`, `eachchunk`, `haschunks`
- **Async chunk I/O**: Channel-based parallel chunk reads/writes for stores supporting `ConcurrentRead`

### Storage Interface Requirements

New store backends must implement: `getindex(store, key)::Union{Vector{UInt8}, Nothing}`, `setindex!(store, data, key)`, `storagesize(store, path)`, `subdirs(store, path)`, `subkeys(store, path)`, `isinitialized(store, key)`, `storefromstring(Type, string, create)`.

### V3 Status (Experimental)

V3 support is under active development. Current state:

**Codecs (`lib/ZarrCore/src/Codecs/V3/V3.jl`)**
- `BytesCodec` — stores `endian::Symbol` (`:little` or `:big`); encode/decode byte-swap elements when the target endian differs from the system byte order (`Base.ENDIAN_BOM`). Default is `:little`.
- `TransposeCodec` — array→array permutation codec (renamed from `TransposeCodecImpl`)
- `BloscV3Codec` — shuffle stored as integer (0=noshuffle, 1=shuffle, 2=bitshuffle); parsed from spec strings (`"noshuffle"`, `"shuffle"`, `"bitshuffle"`) and serialized back to strings
- Sharding codec (`sharding_indexed`) has struct definitions and encode/decode logic but is not yet wired into the main read/write pipeline (throws `ArgumentError` when encountered)
- `crc32c` codec has encode/decode implementations and is parseable from metadata

**Metadata (`lib/ZarrCore/src/metadata3.jl`)**
- `MetadataV3{T,N,P}` has no `order` field; storage order is encoded in the pipeline via `TransposeCodec`
- Two constructors:
  - Primary inner constructor: `MetadataV3{T,N,P}(zarr_format, node_type, shape, chunks, dtype, pipeline, fill_value, chunk_encoding)` — takes a pre-built pipeline, no `order` argument
  - Convenience outer constructor: `MetadataV3{T,N}(...; order, endian, compressor, chunk_encoding)` — builds the pipeline from `order` (→ `TransposeCodec`), `endian` (→ `BytesCodec`), and `compressor` (→ bytes→bytes codecs)
- `get_order(md::MetadataV3)` — derives `'C'`/`'F'` from the pipeline; throws `ArgumentError` when order is ambiguous: multiple array→array codecs, an unrecognized array→array codec, or a `TransposeCodec` with a permutation that is neither the identity (C) nor the full reversal (F)
- `get_order(md::MetadataV2)` — returns `md.order`
- `==` for `MetadataV3` compares `pipeline` (not `order`)

**Other V3 status**
- V3 groups work via `zarr.json` with `node_type: "group"`
- `zgroup()` currently always creates v2 groups (hardcoded `DV` format)
- Filters are not implemented for v3 (`filters = nothing` hardcoded in `Metadata3`)
- Test fixtures: `test/v3_julia.jl` (Julia-generated), `test/v3_python.jl` (Python-generated via PythonCall)
- V3 codec tests are in `test/v3_codecs.jl`

### Extension System

S3 support is a weak dependency extension (`ext/ZarrAWSS3Ext.jl`), loaded only when `AWSS3` is imported. The extension registers a regex in `storageregexlist` for auto-detection of `s3://` URLs.
