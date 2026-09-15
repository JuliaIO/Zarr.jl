# ZarrZip.jl

Read-only ZIP storage and ZIP archive writing for Zarr arrays and groups.

## What it exposes

- `ZarrZip.ZipStore(data::AbstractVector{UInt8})`: opens ZIP archive bytes as
  a read-only store. For a file, use `ZipStore(read("archive.zip"))`.
- `ZarrZip.writezip(io, array_or_group)` and `ZarrZip.writezip(io, store, path="")`:
  write an array, group, or store subtree to a ZIP archive. Entry names retain
  their store paths; pass the same `path` to `zopen` for a nested dataset.
- `ZarrZip.register!()`: a no-op provided for consistency with the other backends;
  ZIP access does not require registry entries.

These names are public but not exported. Make updates in a writable store,
then write an archive with `writezip`.

## Quick example

Round-trip an array through an in-memory ZIP archive:

```julia
using ZarrCore, ZarrZip

a = zcreate(Int32, 4; chunks=(2,))
a[:] = Int32[10, 20, 30, 40]

io = IOBuffer()
ZarrZip.writezip(io, a)
b = zopen(ZarrZip.ZipStore(take!(io)), "r")
b[2:3] # [20, 30]
```

[Main README](../README.md) · [Documentation](https://juliaio.github.io/Zarr.jl/) · [License](../LICENSE.md)
