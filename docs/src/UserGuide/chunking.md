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
extension with an inline `chunk_shapes` configuration. A regular axis is
written as its chunk size, an irregular axis as its list of edge lengths with
repeated sizes run-length encoded. This layout can be exchanged with
zarr-python.

Each storage chunk is encoded at its own size rather than padded to a global
maximum. As in a regular grid, the final chunk of a regular axis is stored at
the full chunk size even when the axis length is not a multiple of it.

The specification also allows a list of edge lengths to overflow the array
extent. Zarr.jl does not support this and throws an `ArgumentError` when
opening such an array.

The grid survives reopening:

````jldoctest rectilinear-chunks
julia> dir = joinpath(mktempdir(), "rectilinear.zarr");

julia> z = zcreate(Int, 5, 20; zarr_format=3, chunks, path=dir);

julia> reopened = zopen(dir);

julia> DiskArrays.eachchunk(reopened) == chunks
true
````

## Resizing

`resize!` and `append!` work along the regular axes of a rectilinear array:

````jldoctest rectilinear-chunks
julia> append!(z, fill(7, 3, 20); dims=1)

julia> size(z)
(8, 20)
````

Changing the extent of an irregular axis would require defining new edge
lengths, so Zarr.jl throws an `ArgumentError` before modifying the array.
