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

Since there is no `public` on 1.10, `names()` cannot report public names there, so `@public` *also* appends to the calling module's `PUBLIC_NAMES::Vector{Symbol}`. **Any module that uses `@public` must define its own `const PUBLIC_NAMES = Symbol[]`** (per-module, exactly like `public` itself); forgetting it is a load-time `UndefVarError`. The `Zarr` facade unions `names(ZarrCore)` with `ZarrCore.PUBLIC_NAMES`, which is what keeps the public API present on LTS — without it, every public-but-not-exported name silently vanishes from `Zarr` on 1.10 while 1.11+ looks fine.

## Architecture

### Core Type Hierarchy

```
AbstractMetadata{T,N}
├── MetadataV2{T,N,C,F}   # .zarray + .zattrs (v2)
└── MetadataV3{T,N,P}     # zarr.json (v3), P<:AbstractCodecPipeline

AbstractStore
├── DirectoryStore          # Filesystem
├── DictStore               # In-memory Dict{String,Vector{UInt8}}
├── S3Store                 # AWS S3 (lives in the ZarrS3 subpackage, implemented by its AWSS3 extension)
├── ConsolidatedStore       # Consolidated metadata wrapper
├── CachingStore            # Local cache in front of a remote store
├── GCStore                 # Google Cloud Storage (lives in the ZarrGCS subpackage)
├── HTTPStore               # HTTP-based read-only (lives in the ZarrHTTP subpackage)
└── ZipStore                # ZIP archive read-only (lives in the ZarrZip subpackage)

ZArray{T,N,S<:AbstractStore,M<:AbstractMetadata} <: AbstractDiskArray{T,N}
ZGroup{S<:AbstractStore}
```

### Package Layout

The repo is a Pkg workspace:

- `ZarrCore` (`lib/ZarrCore/`) — the base implementation. Almost every type, method and docstring lives here.
- `ZarrHTTP` (`lib/ZarrHTTP/`) — the HTTP backend (`HTTPStore`) and the small Zarr server (`zarr_req_handler`, the `HTTP.serve` methods for stores/arrays/groups). Depends on HTTP.jl and OpenSSL.jl.
- `ZarrGCS` (`lib/ZarrGCS/`) — the Google Cloud Storage backend (`GCStore`, `gcs_credentials`). Talks to GCS with HTTP.jl directly, so it depends on the HTTP *package*, not on `ZarrHTTP`.
- `ZarrS3` (`lib/ZarrS3/`) — the S3 backend. The only subpackage with an extension: it owns the `S3Store` type, the "load AWSS3" fallback constructor and the `^s3://` registration, while every method lives in `lib/ZarrS3/ext/ZarrS3AWSS3Ext.jl`, which loads only once AWSS3.jl does.
- `ZarrZip` (`lib/ZarrZip/`) — the zip backend (`ZipStore`, `writezip`), extracted from ZarrCore.
- `ZarrBlosc` (`lib/ZarrBlosc/`) — `BloscCompressor` (v2) + `BloscV3Codec` (v3), backed by Blosc.jl.
- `ZarrZlib` (`lib/ZarrZlib/`) — `ZlibCompressor` (v2) + `GzipV3Codec` (v3; the specs spell the same algorithm differently), backed by ChunkCodecLibZlib.jl.
- `ZarrZstd` (`lib/ZarrZstd/`) — `ZstdCompressor` (v2) + `ZstdV3Codec` (v3), backed by ChunkCodecLibZstd.jl.
- `Zarr` (`src/Zarr.jl`) — a thin facade that re-exports the API of every subpackage listed in `Zarr.REEXPORTED_MODULES`. It mirrors each one's export/public split: names a subpackage exports are re-exported, names it only marks `@public` stay public (not exported). Nothing else is forwarded, so internals must be reached as `Zarr.ZarrCore.foo` / `Zarr.ZarrHTTP.foo`. Its `__init__` also sets `ZarrCore.DEFAULT_COMPRESSOR[] = ZarrBlosc.BloscCompressor()`; a bare `ZarrCore` defaults to `NoCompressor()`.

`ZarrCore` now depends only on CRC32c, DateTimes64, Dates, DiskArrays, JSON, OrderedCollections and Unicode. It contains no store that is addressable by a URL scheme, so `ZarrCore.storageregexlist` is empty in a bare `ZarrCore` session; every entry is pushed by a subpackage's `__init__`.

**`ZarrS3` is a hard dep of `Zarr` but keeps AWSS3 weak.** AWSS3 costs ~330 ms to load — more than the rest of Zarr put together — and most sessions never touch S3, so it stays a weak dependency; it just moved down from the umbrella to `ZarrS3`, which declares it in its own `[weakdeps]`/`[extensions]`. `ZarrS3` itself is a normal dependency of `Zarr`, so `Zarr.S3Store` always resolves, `S3Store("bucket")` without AWSS3 still gives the friendly `error("AWSS3 must be loaded to use S3Store. Try \`using AWSS3\`.")`, and `zopen("s3://…")` still reaches that same message rather than "no storage type matched". Behaviour is identical to when the extension hung off the umbrella; only the package that owns it changed.

Subpackages depend **on** ZarrCore, never the reverse. Each one owns its own `PUBLIC_NAMES` registry, declares its API with `ZarrCore.@public`, and must extend ZarrCore generics with a qualified definition (`ZarrCore.subkeys(d::ZipStore, p) = ...`) — a bare `subkeys(...)` definition creates a shadowing function instead of a method, and `zopen` would never call it.

**A generic with methods on both sides of the split must be *declared* in ZarrCore.** If a subpackage adds methods to a function that also keeps methods in ZarrCore (or in another package), ZarrCore must own the binding — as a default method or a bare `function f end` — or each package silently gets its own function and the methods never meet. `missing_chunk_return_code!` (methods on `HTTPStore` in ZarrHTTP, on `CachingStore`/`ConsolidatedStore` in ZarrCore) and `cloud_list_objects` (methods on `GCStore` in ZarrGCS, on `S3Store` in ZarrS3's `ZarrS3AWSS3Ext`) are declared method-less in `Storage/Storage.jl` for exactly this reason.

**Registry mutations must live in `__init__`.** A subpackage that writes into a registry owned by another package (`ZarrCore.compressortypes[...] = ...`, `V3Codecs.register_codec(...)`, `push!(ZarrCore.storageregexlist, ...)`, `ZarrCore.DEFAULT_COMPRESSOR[] = ...`) has to do it from `__init__`, not at top level: a top-level mutation of another module's global state is silently discarded when the precompiled image is written, with no error and no warning, and the entry is simply absent in every fresh session. Verify in a *new* process, not the one that just precompiled.

Adding a subpackage means: a `lib/<Name>/` package, entries in the root `Project.toml` (`[deps]`, `[sources]`, `[workspace] projects`, `[compat]`), one line in `REEXPORTED_MODULES` plus its `import`, the `Pkg.develop` lists in `.github/workflows/CI.yml` (Julia 1.10 ignores `[sources]`), and `modules` in `docs/make.jl` (plus `docs/src/reference.md` if `@autodocs` referenced the moved files by path).

### Public API Policy

Set by [a maintainer comment on PR #317](https://github.com/JuliaIO/Zarr.jl/pull/317#issuecomment-5314176722). Anything a downstream consumer could need, plus every documented extension point, is part of the public API; everything else is internal and may change.

- **Exported** (in scope after `using Zarr`): `ZArray`, `ZGroup`, `zopen`, `zzeros`, `zcreate`, `zgroup`, `zarrcache`, `storagesize`, `storageratio`, `zinfo`, `DirectoryStore`, `S3Store`, `GCStore`.
- **Public but not exported**: every store, codec, filter and compressor type; the store/filter/compressor/codec/chunk-key-encoding extension interfaces; `typestr`, `fill_value_encoding`, `fill_value_decoding`, `zname`, `writezip`, `consolidate_metadata`, `missing_chunk_return_code!`, `gcs_credentials`, `DateTime64`.
- **Internal**: `Metadata`/`MetadataV2`/`MetadataV3`, `ZarrFormat`, `DV`, `is_zarray`, `is_zgroup`, `normalize_path`, `MaxLengthString`, `ASCIIChar`, `ShapeOnlyArray` (should be removed), `getattrs`/`writeattrs`/`getmetadata`/`writemetadata`, `V2Pipeline`/`V3Pipeline`/`pipeline_encode`/`pipeline_decode!`, and the `store_*` helpers.

Tests follow the same rule: public names are used as `Zarr.foo`, internals as `ZarrCore.foo` (each test file does `import Zarr: ZarrCore`). If a test needs `ZarrCore.` for something a downstream user would plausibly need, that is a signal the name should be made public rather than the test qualified.

### Module/File Layout

Each single-file subpackage holds its whole implementation in `lib/<Name>/src/<Name>.jl`: `ZarrHTTP` the HTTP backend and server, `ZarrGCS` the GCS backend, `ZarrZip` the zip backend (`ZipStore`, `writezip`, `_make_prefix`), and `ZarrBlosc`/`ZarrZlib`/`ZarrZstd` a v2 compressor, its v3 codec, the `v2_to_v3_codecs` method joining them, and an `__init__` doing both registrations. `ZarrS3` is the exception: `src/ZarrS3.jl` holds only the `S3Store` struct, the "load AWSS3" fallback constructor and the `^s3://` registration, and every method lives in `ext/ZarrS3AWSS3Ext.jl` + `ext/s3store.jl`. All other paths below are relative to `lib/ZarrCore/`.

- `src/ZarrCore.jl` — Module entry point, defines `ZarrFormat{V}` (Val-parameterized version tag, default `DV = ZarrFormat(Val(2))`), the `@public` macro, and the export/public declarations
- `src/metadata.jl` — `MetadataV2` struct, type string encoding (`typestr`), fill value encoding/decoding, `Metadata()` constructors for V2; dispatches V3 to `metadata3.jl`
- `src/metadata3.jl` — All V3-specific code: `MetadataV3` struct and constructors, `Metadata3(dict)` parsing, `lower3` serialization, codec pipeline parsing, `get_order`, `JSON.lower(::MetadataV3)`
- `src/chunkkeyencoding.jl` — `ChunkKeyEncoding` struct (separator char + prefix bool), `citostring()` for chunk path generation, plus the `register_chunk_key_encoding` registry. V2 default: `'.'` separator, no prefix. V3 default: `'/'` separator, `"c/"` prefix
- `src/ZArray.jl` — Core array type, `readblock!`/`writeblock!` (DiskArrays interface), `zcreate`, `zzeros`, `zopen`, resize/append
- `src/ZGroup.jl` — Hierarchical group support, `zopen`, `zgroup`, auto-detection of zarr version via `ZarrFormat(store, path)`
- `src/Compressors/Compressors.jl` — `Compressor` abstract type, `NoCompressor`, the `compressortypes` registry (keyed by spec name string), `DEFAULT_COMPRESSOR`, the `zcompress`/`zuncompress`/`getCompressor`/`v2_to_v3_codecs` generics. Concrete compressors live in `ZarrBlosc`/`ZarrZlib`/`ZarrZstd`
- `src/Codecs/` — V3 codec system (`Codec` abstract type), `V3/V3.jl` defines `V3Codec{In,Out}` with `BytesCodec`, `CRC32cV3Codec`, `ShardingCodec`, `TransposeCodec`, `VLenUTF8V3Codec`, and the `register_codec` registry. `BloscV3Codec`, `GzipV3Codec` and `ZstdV3Codec` live in the compressor subpackages
- `src/Filters/` — `Filter{T,TENC}` abstract type, implementations for variable-length arrays, strings, Fletcher32, shuffle, delta, quantize
- `src/Storage/Storage.jl` — `AbstractStore` interface, I/O strategy (`SequentialRead`/`ConcurrentRead`), chunk read/write/delete helpers, metadata read/write dispatched on `ZarrFormat{2}` vs `ZarrFormat{3}`, the `storageregexlist` registry (a `StoreRegexList`, kept sorted most-specific-first so registration order does not matter), and the method-less `cloud_list_objects`/`missing_chunk_return_code!` generics

### Key Design Patterns

- **Format version dispatch**: `ZarrFormat{V}` (where V is 2 or 3) used throughout for multiple dispatch on version-specific behavior (metadata file names, chunk encoding, serialization format)
- **Shape is mutable**: `metadata.shape` is `Base.RefValue{NTuple{N,Int}}` to allow `resize!` without replacing the metadata struct
- **Column-major ↔ row-major**: Zarr stores shapes/chunks in row-major (C order); Julia uses column-major. All conversions happen via `reverse()` at metadata boundaries
- **Compressor registry**: `compressortypes` dict maps spec names (e.g., `"blosc"`, `"zlib"`) to compressor types. V3 uses `Compressor_v3{C}` wrapper to change JSON serialization format
- **DiskArrays integration**: `ZArray <: AbstractDiskArray`, chunk-aware I/O via `readblock!`/`writeblock!`, `eachchunk`, `haschunks`
- **Async chunk I/O**: Channel-based parallel chunk reads/writes for stores supporting `ConcurrentRead`

### Storage Interface Requirements

New store backends must implement: `getindex(store, key)::Union{Vector{UInt8}, Nothing}`, `setindex!(store, data, key)`, `storagesize(store, path)`, `subdirs(store, path)`, `subkeys(store, path)`, `isinitialized(store, key)`, `storefromstring(Type, string, create)`. A backend addressable by a URL scheme also registers `push!(storageregexlist, r"^myproto://" => MyStore)` — from `__init__` if it lives outside ZarrCore. Entries are ranked by pattern specificity, not registration order, so `r"^https://storage.googleapis.com"` beats `r"^https://"` no matter which package loads first.

### V3 Status (Experimental)

V3 support is under active development. Current state:

**Codecs (`lib/ZarrCore/src/Codecs/V3/V3.jl`)**
- `BytesCodec` — stores `endian::Symbol` (`:little` or `:big`); encode/decode byte-swap elements when the target endian differs from the system byte order (`Base.ENDIAN_BOM`). Default is `:little`.
- `TransposeCodec` — array→array permutation codec (renamed from `TransposeCodecImpl`)
- `BloscV3Codec` (in `lib/ZarrBlosc/`) — shuffle stored as integer (0=noshuffle, 1=shuffle, 2=bitshuffle); parsed from spec strings (`"noshuffle"`, `"shuffle"`, `"bitshuffle"`) and serialized back to strings
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

S3 support is a weak-dependency extension (`lib/ZarrS3/ext/ZarrS3AWSS3Ext.jl`), loaded only when `AWSS3` is imported. It hangs off `ZarrS3`, not the `Zarr` umbrella, so `Zarr`'s own `Project.toml` has no `[weakdeps]`/`[extensions]` at all. The `^s3://` regex is registered by `ZarrS3.__init__` rather than by the extension, so URL detection works before AWSS3 is loaded.
