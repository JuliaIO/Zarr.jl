# Zarr.jl

The facade package for reading and writing chunked, compressed Zarr v2 and v3
arrays. Loading `Zarr` loads all the codec and storage packages in this repository.

## What it exposes

- Arrays and groups: `ZArray`, `ZGroup`, `zcreate`, `zzeros`, `zopen`, and `zgroup`.
- Caching and inspection: `zarrcache`, `zinfo`, `storagesize`, and `storageratio`.
- Storage: `DirectoryStore`, `GCStore`, and `S3Store`, plus qualified names
  `Zarr.DictStore`, `Zarr.HTTPStore`, `Zarr.ZipStore`, `Zarr.CachingStore`, and
  `Zarr.ConsolidatedStore`. S3 access also requires `using AWSS3`.
- Compression: `Zarr.BloscCompressor`, `Zarr.ZlibCompressor`, and
  `Zarr.ZstdCompressor`, plus their v3 codecs and the core filters and codecs.
  Blosc is the default compressor.
- Helpers including `Zarr.consolidate_metadata`, `Zarr.writezip`, and
  `Zarr.gcs_credentials`.

The facade preserves each package's distinction between exported names and
public names that require a prefix. Registration is package-specific: use, for
example, `Zarr.ZarrBlosc.register!()` if automatic registration is disabled.
For a smaller dependency set, use [ZarrCore](../ZarrCore) with only the backend
and compressor packages you need.

## Quick example

Create a local array, write values, and reopen it for reading:

```julia
using Zarr

path = joinpath(mktempdir(), "example.zarr")
a = zcreate(Int32, 4; path, chunks=(2,), zarr_format=3)
a[:] = Int32[10, 20, 30, 40]

b = zopen(path, "r")
b[2:3] # [20, 30]
```

[Main README](../README.md) · [Documentation](https://juliaio.github.io/Zarr.jl/) · [License](../LICENSE.md)
