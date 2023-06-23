## Operations on Zarr Arrays

A Zarr Array consists of a collection of potentially compressed chunks, and there is a significant overhead in accessing a single item from such an array compared to Julia's Base Array type.

In order to make operations on `ZArray`s still efficient, we use the [DiskArrays](https://github.com/meggart/DiskArrays.jl/) package which enables efficient broadcast and reductions on `Zarray`s respecting their chunk sizes. This includes some modified behavior compared to a normal `AbstractArray`, including lazy broadcasting and a non-default array access order for reductions.

Please refer to the DiskArrays documentation to see which operations are supported.

### A short example

````@jldoctest
julia> using Zarr, Statistics

julia> g = zopen("gs://cmip6/CMIP/NCAR/CESM2/historical/r9i1p1f1/Amon/tas/gn/", consolidated=true)
ZarrGroup at Consolidated S3 Object Storage
Variables: lat time tas lat_bnds lon_bnds lon time_bnds
````

Accessing a single element from the array has significant overhead, because a whole chunk has to be transferred from GCS and unzipped:

````julia
julia> @time g["tas"][1,1,1]
````
````
18.734581 seconds (129.25 k allocations: 557.614 MiB, 0.56% gc time)

244.39726f0
````


````@jldoctest
julia> latweights = reshape(cosd.(g["lat"])[:],1,192,1);

julia> t_celsius = g["tas"].-273.15
Disk Array with size 288 x 192 x 1980

julia> t_w = t_celsius .* latweights
Disk Array with size 288 x 192 x 1980
````

Note that the broadcast operations are not directly computed but are collected in a fused lazy Broadcast object. When calling a reducing operation on the array, it will be read chunk by chunk and means will be merged instead of accessing the elements in a naive loop, so that the computation can be finished in reasonable time:

````@jldoctest
julia> mean(t_w, dims = (1,2))./mean(latweights)
1×1×1980 Array{Float64,3}:
[:, :, 1] =
 12.492234157689309

[:, :, 2] =
 12.425466417315654

[:, :, 3] =
 13.190267552582446

...

[:, :, 1978] =
 15.55063620093181

[:, :, 1979] =
 14.614388350826788

[:, :, 1980] =
 13.913361540597469
````
