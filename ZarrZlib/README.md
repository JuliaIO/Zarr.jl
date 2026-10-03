# ZarrZlib.jl

Zlib compression for Zarr v2 and gzip compression for Zarr v3, usable with
ZarrCore directly.

## What it exposes

- `ZarrZlib.ZlibCompressor(; clevel=-1)` (also `ZlibCompressor(level)`):
  the v2 `zlib` compressor. Levels are 0–9; `-1` selects the library default.
- `ZarrZlib.GzipV3Codec(level=6)`: the v3 `gzip` codec, with levels 0–9.
- `ZarrZlib.register!()`: registers v2 `zlib` and v3 `gzip` with ZarrCore.
  This runs automatically unless ZarrCore's `RegisterAtInit` preference is disabled.

These names are public but not exported. The v2 and v3 implementations use
different stream formats (zlib and gzip, respectively).

## Quick example

```julia
using ZarrCore, ZarrZlib

a = zcreate(Int32, 4; chunks=(2,),
    compressor=ZarrZlib.ZlibCompressor(clevel=6))
a[:] = Int32[10, 20, 30, 40]
a[2:3] # [20, 30]
```

This creates an in-memory v2 array. Add `zarr_format=3` to `zcreate` to use
gzip through the same compressor argument; level `-1` maps to gzip level 6.

[Main README](../README.md) · [Documentation](https://juliaio.github.io/Zarr.jl/) · [License](../LICENSE.md)
