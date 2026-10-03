# ZarrBlosc.jl

Blosc compression for Zarr v2 and v3 arrays, usable with ZarrCore directly.

## What it exposes

- `ZarrBlosc.BloscCompressor(; blocksize=0, clevel=5, cname="lz4", shuffle=1)`:
  the v2 `blosc` compressor. Options control compression level, algorithm,
  block size, and byte/bit shuffling.
- `ZarrBlosc.BloscV3Codec(cname, clevel, shuffle, blocksize, typesize)`:
  the v3 `blosc` codec; `BloscV3Codec()` uses `("lz4", 5, 1, 0, 4)`.
- `ZarrBlosc.register!()`: registers both implementations with ZarrCore.
  This runs automatically unless ZarrCore's `RegisterAtInit` preference is disabled.

These names are public but not exported. Loading ZarrBlosc also makes
`BloscCompressor()` the default compressor for ZarrCore arrays.

## Quick example

```julia
using ZarrCore, ZarrBlosc

a = zcreate(Int32, 4; chunks=(2,),
    compressor=ZarrBlosc.BloscCompressor(clevel=5, cname="lz4"))
a[:] = Int32[10, 20, 30, 40]
a[2:3] # [20, 30]
```

This creates an in-memory v2 array. Add `zarr_format=3` to `zcreate` to use
the corresponding v3 codec through the same compressor argument.

[Main README](../README.md) · [Documentation](https://juliaio.github.io/Zarr.jl/) · [License](../LICENSE.md)
