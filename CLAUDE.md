# CLAUDE.md

## Project Overview

Zarr.jl implements Zarr v2 and experimental v3 arrays with filesystem, memory, HTTP, GCS, S3, and ZIP storage.

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

Julia version requirement: 1.10+. CI tests LTS, stable, nightly, and prerelease Julia on Ubuntu, plus stable Julia on macOS and Windows.

### Public names

Each module stores public-only declarations in an adjacent `public_names_*.jl`
containing only `public` statements and comments. Include it only on Julia 1.11+;
`Zarr._declared_public_names` parses it on Julia 1.10.
`src/public_names_zarr.jl` defines the facade's public subset.

## Architecture

### Core Type Hierarchy

```
AbstractMetadata{T,N}
├── MetadataV2{T,N,C,F}   # .zarray + .zattrs (v2)
└── MetadataV3{T,N,P}     # zarr.json (v3), P<:AbstractCodecPipeline

AbstractStore
├── DirectoryStore          # Filesystem
├── DictStore               # In-memory Dict{String,Vector{UInt8}}
├── S3Store                 # S3 via ZarrS3/AWSS3
├── ConsolidatedStore       # Consolidated metadata wrapper
├── CachingStore            # Local cache in front of a remote store
├── GCStore                 # GCS via ZarrGCS
├── HTTPStore               # HTTP via ZarrHTTP
└── ZipStore                # ZIP via ZarrZip

ZArray{T,N,S<:AbstractStore,M<:AbstractMetadata} <: AbstractDiskArray{T,N}
ZGroup{S<:AbstractStore}
```

### Package Layout

The repo is a Pkg workspace:

- `ZarrCore` — core types, metadata, filters, storage interfaces, and codec interfaces.
- `ZarrHTTP` — `HTTPStore` and HTTP serving methods.
- `ZarrGCS` — `GCStore` and GCS credentials.
- `ZarrS3` — `S3Store`; AWSS3 methods load through a weak extension.
- `ZarrZip` — `ZipStore` and `writezip`.
- `ZarrBlosc`, `ZarrZlib`, `ZarrZstd` — v2 compressors and matching v3 codecs; `ZarrBlosc` provides the default compressor.
- `Zarr` — facade that re-exports `REEXPORTED_MODULES`.

Package rules:

- Subpackages depend on `ZarrCore`; `ZarrCore` does not depend on them.
- Extend ZarrCore generics with qualified definitions. Declare shared generics in ZarrCore.
- Mutate cross-package registries in runtime `__init__` so entries survive
  precompilation. Guard optional automatic registration with
  `ZarrCore.should_register_at_init() && register!()` and expose a documented,
  public, package-qualified `register!` that works regardless of the preference.
- `RegisterAtInit` defaults to `true`. To disable it, set
  `[ZarrCore] RegisterAtInit = false` in the active environment's
  `LocalPreferences.toml`, then restart Julia. Manual registration affects only
  the current process. Core built-ins remain registered, and the preference
  does not unload types or methods or disable Blosc default-compressor dispatch.
- Downstream packages that register explicitly must call the qualified
  `register!` from their runtime `__init__`; calls made only during
  precompilation do not restore registry entries when loaded.
- Do not re-export extension `register!` functions from the `Zarr` facade;
  callers use `Zarr.ZarrBlosc.register!()` (and the corresponding package
  module) or import the extension package directly.
- Keep AWSS3 as a weak dependency of `ZarrS3`.
- New subpackages require root project entries, a `public_names_*.jl`, a
  `REEXPORTED_MODULES` entry, CI develop paths, and a Documenter module entry.

### Public API Policy

Set by [a maintainer comment on PR #317](https://github.com/JuliaIO/Zarr.jl/pull/317#issuecomment-5314176722).
Downstream functionality and documented extension points are public; other names
are internal.

- **Exported**: `ZArray`, `ZGroup`, `zopen`, `zzeros`, `zcreate`, `zgroup`,
  `zarrcache`, `storagesize`, `storageratio`, `zinfo`, `DirectoryStore`,
  `S3Store`, `GCStore`.
- **Public but not exported**: store, codec, filter, and compressor types;
  extension interfaces; `typestr`, fill-value functions, `zname`, `writezip`,
  `consolidate_metadata`, `missing_chunk_return_code!`, and `gcs_credentials`.
- **Internal**: metadata structs, `ZarrFormat`, `PermanentZarrCache`, detection and
  normalization helpers, codec pipelines, and `store_*` helpers.

Tests use `Zarr.foo` for public names and `ZarrCore.foo` for internals.

### Module/File Layout

Core paths below are relative to `lib/ZarrCore/`. Backend implementations are in
`lib/<Name>/src/<Name>.jl`; S3 data-access methods are in `lib/ZarrS3/ext/`.

- `src/ZarrCore.jl` — module entry point, `ZarrFormat`, exports, and public names.
- `src/metadata.jl` — v2 metadata, dtype conversion, and fill-value conversion.
- `src/metadata3.jl` — v3 metadata, codec pipelines, ordering, and serialization.
- `src/chunkkeyencoding.jl` — chunk-key encodings and registry.
- `src/ZArray.jl` — Core array type, `readblock!`/`writeblock!` (DiskArrays interface), `zcreate`, `zzeros`, `zopen`, resize/append
- `src/ZGroup.jl` — Hierarchical group support, `zopen`, `zgroup`, auto-detection of zarr version via `ZarrFormat(store, path)`
- `src/Compressors/Compressors.jl` — compressor interface, registry, and default.
- `src/Codecs/` — core v3 codecs and codec registry.
- `src/Filters/` — filter interface and implementations.
- `src/Storage/Storage.jl` — store interface, I/O strategies, chunk operations,
  URL registry, and shared backend generics.

### Key Design Patterns

- **Format version dispatch**: `ZarrFormat{V}` selects version-specific behavior
- **Shape is mutable**: `metadata.shape` is `Base.RefValue{NTuple{N,Int}}` to allow `resize!` without replacing the metadata struct
- **Column-major ↔ row-major**: reverse dimensions at metadata boundaries
- **Compressor registry**: `compressortypes` maps v2 names to compressor types; `v2_to_v3_codecs` maps compressors to v3 codecs by dispatch
- **DiskArrays integration**: `ZArray <: AbstractDiskArray`, chunk-aware I/O via `readblock!`/`writeblock!`, `eachchunk`, `haschunks`
- **Async chunk I/O**: Channel-based parallel chunk reads/writes for stores supporting `ConcurrentRead`

### Storage Interface Requirements

New stores implement `getindex`, `setindex!`, `storagesize`, `subdirs`,
`subkeys`, `isinitialized`, and `storefromstring`. URL-addressable stores
expose a qualified `register!` and invoke it from runtime `__init__`, guarded by
`ZarrCore.should_register_at_init()`; longer URL patterns take precedence.

### V3 Status

V3 support is experimental. Arrays, groups, codec pipelines, sharding, endian
conversion, and Julia/Python fixtures are implemented. `zgroup()` still creates
v2 groups, and v3 filters are unsupported.
