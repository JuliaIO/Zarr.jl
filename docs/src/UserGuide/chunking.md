# Regular and Rectilinear Chunking

Zarr arrays divide their data into independently encoded chunks. With regular
chunking, every chunk has the same nominal size along an axis. This is the
layout selected by a tuple-valued `chunks` keyword:

````jldoctest regular-chunks
julia> using Zarr

julia> z = zcreate(Int, 100, 80; chunks=(10, 20))
ZArray{Int64} of size 100 x 80
````

## Rectilinear grids

Zarr v3 rectilinear grids allow chunk sizes to vary independently along each
axis. Zarr.jl represents these grids with the chunk types from DiskArrays.jl:

````jldoctest rectilinear-chunks
julia> using Zarr

julia> using DiskArrays: GridChunks, IrregularChunks, RegularChunks

julia> import DiskArrays

julia> chunks = GridChunks(
           RegularChunks(2, 0, 5),
           IrregularChunks(chunksizes=[3, 4, 5, 6, 2]),
       );

julia> z = zcreate(Int, 5, 20; zarr_format=3, chunks)
ZArray{Int64} of size 5 x 20

julia> z[:, :] = reshape(1:100, 5, 20);

julia> z[:, :] == reshape(1:100, 5, 20)
true
````

The first axis uses regular chunks of two elements (with a one-element final
chunk). The second uses chunks of 3, 4, 5, 6, and 2 elements. Reads and writes
may cross any number of these boundaries without changing the indexing API.

`zcreate`, `zzeros`, and the `ZArray(data; chunks=...)` constructor accept a
`GridChunks` value. The grid must:

- have one chunk specification per array axis;
- start at offset zero; and
- describe the complete array extent on every axis.

Irregular grids require `zarr_format=3`; Zarr v2 metadata cannot represent
them.

## Metadata and interoperability

Zarr.jl serializes an irregular grid using Zarr v3's `rectilinear` chunk-grid
extension with an inline `chunk_shapes` configuration. Repeated sizes are
run-length encoded, and each storage chunk is encoded at its actual grid
extent rather than padded to a global maximum. This layout can be exchanged
with zarr-python.

The grid survives reopening:

````jldoctest rectilinear-chunks
julia> dir = joinpath(mktempdir(), "rectilinear.zarr");

julia> z = zcreate(Int, 5, 20; zarr_format=3, chunks, path=dir);

julia> reopened = zopen(dir);

julia> DiskArrays.eachchunk(reopened) == chunks
true
````

## Resizing

`resize!` and `append!` are not supported for rectilinear arrays. Changing the
array extent would also require defining new per-axis chunk lengths, so Zarr.jl
throws an `ArgumentError` before modifying the array. Regular arrays remain
resizable.
