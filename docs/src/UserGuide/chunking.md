# Chunking and Irregular Chunk Grids

A Zarr array is divided into chunks, each of which is compressed and stored (and
transferred) independently. The chunk size along every axis is therefore one of
the main knobs controlling read/write performance and storage granularity.

By default chunks have a uniform size along each axis (regular chunking), which
is what the plain tuple form of the `chunks` keyword gives you:

````jldoctest regular
julia> using Zarr

julia> z = zzeros(Int, 100, 100; chunks=(10, 20))
ZArray{Int64} of size 100 x 100
````

Here the array is divided into chunks of 10×20 elements, evenly tiling the
array.

## Controlling the chunk grid with `GridChunks`

Passing a tuple only fixes the chunk size; to fully control the chunk grid you
can instead pass a `DiskArrays.GridChunks` object as the `chunks` keyword to
[`zcreate`](@ref), [`zzeros`](@ref) or the `ZArray(a::AbstractArray, ...; chunks=...)`
constructor. This is also what makes *irregular* (rectilinear) chunking
possible, where chunk sizes vary along an axis instead of being uniform.

The chunk types and the `GridChunks` wrapper come from the
[DiskArrays.jl](https://github.com/JuliaIO/DiskArrays.jl) package, which Zarr
builds on. Bring them into scope with:

```julia
using DiskArrays: GridChunks, IrregularChunks, RegularChunks
```

A `GridChunks` holds one chunk specification per axis, either a
`RegularChunks` or an `IrregularChunks`. Irregular chunks along an axis are
described by their edge lengths:

````jldoctest irregular
julia> using Zarr

julia> using DiskArrays: GridChunks, IrregularChunks, RegularChunks

julia> chunks = GridChunks(RegularChunks(2, 0, 5), IrregularChunks(chunksizes=[3, 4, 5, 6, 2]));

julia> z = zcreate(Int, 5, 20; zarr_format=3, chunks=chunks)
ZArray{Int64} of size 5 x 20

julia> eachchunk(z)
GridChunks(
RegularChunks(2, 0, 5)IrregularChunks([0, 3, 7, 12, 18, 20]))

julia> z[:, :] = reshape(1:100, 5, 20);

julia> z[:, :] == reshape(1:100, 5, 20)
true
````

The second axis is split into chunks of 3, 4, 5, 6 and 2 elements (20 in total),
while the first axis keeps regular chunks of size 2.

`zzeros` works the same way; for grids that are not uniformly regular it fills
the array element by element rather than reusing a single encoded chunk:

````jldoctest irregular
julia> zz = zzeros(Int, 10, 10; zarr_format=3, chunks=GridChunks(RegularChunks(5, 0, 10), IrregularChunks(chunksizes=[3, 3, 4])));

julia> zz[1, 1]
0
````

## Persistence and interoperability

Irregular chunk grids are stored in the array metadata, so they survive
round-tripping through a store. Zarr v3 records them as a `rectilinear` chunk
grid with `kind: "inline"`, where each axis is either a single chunk size
(regular) or a list of edge lengths. Reopening the array restores the exact
grid:

````jldoctest irregular
julia> dir = joinpath(mktempdir(), "irregular.zarr");

julia> z = zcreate(Int, 5, 20; zarr_format=3, chunks=chunks, path=dir);

julia> z2 = zopen(dir);

julia> eachchunk(z2)
GridChunks(
RegularChunks(2, 0, 5)IrregularChunks([0, 3, 7, 12, 18, 20]))
````

::: warning

Zarr v2 has no concept of irregular chunk grids. If you create an array with an
irregular grid and persist it as v2, the metadata only records the *maximum*
chunk size along each axis, and the array is reopened as a regular grid of that
size. Irregular chunking is therefore only preserved end-to-end with Zarr v3
(`zarr_format=3`).

:::

::: warning

`resize!` and `append!` are not supported for arrays whose grid contains
`IrregularChunks`; shrinking such an array throws an `ArgumentError`. Resizing
an irregular grid would require redefining the per-axis edge lengths, which is
not implemented.

:::
