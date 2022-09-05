# Dealing with FillValues

In Zarr metadata, a fillvalue is specified for every array. This means that, when creating an empty array, uninitialized chunks will be assumed to be filled with this value. For example:

````jldoctest fillval
julia> using Zarr

julia> p = tempname();

julia> z = zcreate(Int64, 100, 100, path = p, chunks = (10,10), fill_value=-1)
ZArray{Int64} of size 100 x 100

julia> z[1:2,1]
2-element Vector{Int64}:
 -1
 -1

````

Note that except some array metadata, no chunks will be written to disk in this case. Non-existing chunks are simply interpreted as fillvalues. You can check this with:

````jldoctest fillval
julia> readdir(p)
2-element Vector{String}:
 ".zarray"
 ".zattrs"
````

and only after writing some non-fillvalue data there will be chunks on disk:

````jldoctest fillval
julia> z[1:20,1:10] .= 5
Disk Array with size 20 x 10


julia> readdir(p)
4-element Vector{String}:
 ".zarray"
 ".zattrs"
 "0.0"
 "0.1"
````

Also be aware that during `setindex!`, when chunks only contain FillValues, the chunk will not be written to disk or deleted if it existed before. So if we write `-1`s again into our array, the corresponding chunks will be deleted.

````jldoctest fillval
julia> z[1:10,1:10] .= -1
Disk Array with size 10 x 10


julia> readdir(p)
3-element Vector{String}:
 ".zarray"
 ".zattrs"
 "0.1"
````


## Dealing with Julia's Missing type in Zarr.jl

Like most data storage formats, also Zarr supports storing most of the standard C-compatible data types like integers, unsigned integers and floating point types of different sizes. This Means that it is no problem to directly map a `Vector{Int64}` to a Zarr array. However, the story gets complicated for arrays containing missings with a Union element type like `Union{Int64,Missing}`, since they can not be passed to compression lbraries as simple C pointers and are not very inter-operable with other lanugages. 

One solution to this problem is to use Zarrs `fillvalue`s to represent missing values. Here we open the previously created array and use the `fill_as_missing` option. In this case accessing an uninitialized array member will return missing:

```jldoctest fillval
julia> z = zopen(p, fill_as_missing=true)
ZArray{Union{Missing, Int64}} of size 100 x 100

julia> eltype(z)
Union{Missing, Int64}

julia> z[8:12,1]
5-element reshape(::Matrix{Union{Missing, Int64}}, 5) with eltype Union{Missing, Int64}:
  missing
  missing
  missing
 5
 5

```

The `fill_as_missing` option is also available on array construction with `zcreate`, `zopen` or `zzeros`. 
Note also that one can also write missings into arrays opened with `fill_as_missing=true`. This means that every `missing` entry will be converted to a fillvalue in the zarr array and will appear as fill values in other software that opens the same array. 


