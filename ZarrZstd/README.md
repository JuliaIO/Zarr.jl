# ZarrZstd.jl

Zstandard compression for Zarr v2 and v3 arrays, usable with ZarrCore directly.

## What it exposes

- `ZarrZstd.ZstdCompressor(; level=0, checksum=false)`: the v2 `zstd`
  compressor. Level 0 selects the library default; positive levels trade speed
  for compression, and negative levels favor speed. `checksum` enables a checksum.
- `ZarrZstd.ZstdV3Codec(level=3)`: the v3 `zstd` codec.
- `ZarrZstd.register!()`: registers both implementations with ZarrCore.
  This runs automatically unless ZarrCore's `RegisterAtInit` preference is disabled.

These names are public but not exported. The v3 codec exposes a compression
level only; converting a v2 compressor to v3 does not carry over `checksum`.

## Quick example

```julia
using ZarrCore, ZarrZstd

a = zcreate(Int32, 4; chunks=(2,),
    compressor=ZarrZstd.ZstdCompressor(level=3))
a[:] = Int32[10, 20, 30, 40]
a[2:3] # [20, 30]
```

This creates an in-memory v2 array. Add `zarr_format=3` to `zcreate` to use
the corresponding v3 codec through the same compressor argument.

[Main README](../README.md) · [Documentation](https://juliaio.github.io/Zarr.jl/) · [License](../LICENSE.md)
