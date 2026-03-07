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
julia> z[1:20,1:10] .= 5;

julia> readdir(p)
4-element Vector{String}:
 ".zarray"
 ".zattrs"
 "0.0"
 "0.1"
````
Note that chunk filenames follow the convention `"row_chunk.col_chunk"`, so `"0.0"` covers rows 1–10 / cols 1–10 and `"0.1"` covers rows 11–20 / cols 1–10.

When creating new arrays, uninitialized chunks are not written to disk and are read back as the fill value. However, once a chunk has been written, overwriting it with fill values will keep the chunk on disk (the existing file is updated in place rather than deleted):

````jldoctest fillval
julia> z[1:10,1:10] .= -1;

julia> readdir(p)
4-element Vector{String}:
 ".zarray"
 ".zattrs"
 "0.0"
 "0.1"
````

## Dealing with Julia's Missing type in Zarr.jl
Like most data storage formats, also Zarr supports storing most of the standard C-compatible data types like integers, unsigned integers and floating point types of different sizes. This means that it is no problem to directly map a `Vector{Int64}` to a Zarr array. However, the story gets complicated for arrays containing missings with a Union element type like `Union{Int64,Missing}`, since they can not be passed to compression libraries as simple C pointers and are not very inter-operable with other languages.

One solution to this problem is to use Zarr's `fillvalue`s to represent missing values. Here, we open the previously created array and use the `fill_as_missing` option.  Then, accessing an element whose stored value equals the fill value will return `missing`. 

Since both written chunks hold `5`, all elements in this range read back as `5`. Elements in chunks that were never written, beyond row 20, would return `missing`:

```jldoctest fillval
julia> z = zopen(p, fill_as_missing=true)
ZArray{Union{Missing, Int64}} of size 100 x 100

julia> eltype(z)
Union{Missing, Int64}

julia> z[8:12,1]
5-element Vector{Union{Missing, Int64}}:
 5
 5
 5
 5
 5
```
Accessing a region that was never written returns `missing` instead of the raw fill value:
```jldoctest fillval
julia> z[95:96,1]
2-element Vector{Union{Missing, Int64}}:
 missing
 missing
```
The `fill_as_missing` option is also available on array construction with `zcreate`, `zopen` or `zzeros`.

Note also that one can also write missings into arrays opened with `fill_as_missing=true`. This means that every `missing` entry will be converted to a fillvalue in the zarr array and will appear as fill values in other software that opens the same array.