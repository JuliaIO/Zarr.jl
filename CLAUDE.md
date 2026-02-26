# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Zarr.jl is a Julia implementation of the Zarr specification for chunked, compressed, N-dimensional arrays. It supports Zarr v2 (stable) and v3 (experimental, in development on `fg/continue_v3pr` branch). The package provides multiple storage backends (filesystem, in-memory, S3, GCS, HTTP, ZIP) and compressors (Blosc, zlib, Zstandard).

## Build & Test Commands

```bash
# Run all tests
julia --project -e 'using Pkg; Pkg.test()'

# Run tests directly (equivalent)
julia --project test/runtests.jl

# Run a single test file interactively
julia --project -e 'using Test, Zarr, JSON; include("test/storage.jl")'

# Instantiate dependencies (first time setup)
julia --project -e 'using Pkg; Pkg.instantiate()'
```

Julia version requirement: 1.10+. CI tests against Julia LTS, stable (`1`), nightly, and pre-release on Ubuntu, macOS, and Windows.

## Architecture

### Core Type Hierarchy

```
AbstractMetadata{T,N,C,F}
├── MetadataV2{T,N,C,F}   # .zarray + .zattrs (v2)
└── MetadataV3{T,N,C,F}   # zarr.json (v3)

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

### Module/File Layout

- `src/Zarr.jl` — Module entry point, defines `ZarrFormat{V}` (Val-parameterized version tag, default `DV = ZarrFormat(Val(2))`)
- `src/metadata.jl` — `MetadataV2`, `MetadataV3` structs, type string encoding (`typestr`/`typestr3`), fill value encoding/decoding, `Metadata()` constructors dispatching on `ZarrFormat{2}` vs `ZarrFormat{3}`
- `src/metadata3.jl` — V3-specific metadata parsing (`Metadata3(dict)`) and serialization (`lower3`), codec pipeline parsing
- `src/chunkencoding.jl` — `ChunkEncoding` struct (separator char + prefix bool), `citostring()` for chunk path generation. V2 default: `'.'` separator, no prefix. V3 default: `'/'` separator, `"c/"` prefix
- `src/ZArray.jl` — Core array type, `readblock!`/`writeblock!` (DiskArrays interface), `zcreate`, `zzeros`, `zopen`, resize/append
- `src/ZGroup.jl` — Hierarchical group support, `zopen`, `zgroup`, auto-detection of zarr version via `ZarrFormat(store, path)`
- `src/Compressors/` — `Compressor` abstract type, `compressortypes` registry (keyed by spec name string), implementations: `blosc.jl`, `zlib.jl`, `zstd.jl`, `v3.jl` (v3 wrapper `Compressor_v3{C}`)
- `src/Codecs/` — V3 codec system (`Codec` abstract type), `V3/V3.jl` defines `V3Codec{In,Out}` with `BloscCodec`, `BytesCodec`, `CRC32cCodec`, `GzipCodec`, `ShardingCodec`, `TransposeCodec`
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
- Metadata parsing/serialization works for basic arrays with `bytes`, `blosc`, `gzip`, `zstd`, `transpose` codecs
- Sharding codec (`sharding_indexed`) has struct definitions and encode/decode logic in `Codecs/V3/V3.jl` but is not yet wired into the main read/write pipeline (throws `ArgumentError` when encountered)
- `crc32c` codec has encode/decode implementations but also throws when encountered in metadata parsing
- V3 groups work via `zarr.json` with `node_type: "group"`
- Test fixtures exist in `test/v3_julia.jl` (Julia-generated) and `test/v3_python.jl` (Python-generated via PythonCall), but reference removed `FormattedStore`
- `zgroup()` currently always creates v2 groups (hardcoded `DV` format)
- Filters are not implemented for v3 (`filters = nothing` hardcoded in `Metadata3`)

### Extension System

S3 support is a weak dependency extension (`ext/ZarrAWSS3Ext.jl`), loaded only when `AWSS3` is imported. The extension registers a regex in `storageregexlist` for auto-detection of `s3://` URLs.
