```@meta
CurrentModule = Zarr
DocTestSetup  = quote
    using Zarr
end
```

## Tutorial

Zarr provides classes and functions for working with N-dimensional arrays that behave like Julia arrays but whose data is divided into chunks and each chunk is compressed. If you are already familiar with HDF5 then Zarr arrays provide similar functionality, but with some additional flexibility. This tutorial is an attempt to recreate this  [Python Zarr tutorial](https://zarr.readthedocs.io/en/stable/tutorial.html) as closely as possible and some of the explanation text is just copied and modified from this source.

### Creating an in-memory array

Zarr has several functions for creating arrays. For example:

```jldoctest inmemory
julia> using Zarr

julia> z = zzeros(Int32,10000,10000,chunks=(1000,1000))
ZArray{Int32} of size 10000 x 10000
```

The code above creates a 2-dimensional array of 32-bit integers with 10000 rows and 10000 columns, divided into chunks where each chunk has 1000 rows and 1000 columns (and so there will be 100 chunks in total).

Other Array creation routines are [`zcreate`, `zones` and `zfill`].

### Reading and Writing data

Zarr arrays support a similar interface to Julia arrays for reading and writing data, although they don't implement the all indexing methods of an `AbstractArray` yet. For example, the entire array can be filled with a scalar value:

```jldoctest inmemory
julia> z[:] = 42
42
```

Regions of the array can also be written to, e.g.:

```jldoctest inmemory; output=false
julia> z[1,:]=1:10000;

julia> z[:,1]=1:10000;
```

The contents of the array can be retrieved by slicing, which will load the requested region into memory as a Julia array, e.g.:

```jldoctest inmemory; output=false
julia> z[1,1]
1

julia> z[end,end]
42

julia> z[1,:]
10000-element Array{Int32,1}:
     1
     2
     3
     4
     5
     6
     7
     8
     9
    10
     ⋮
  9992
  9993
  9994
  9995
  9996
  9997
  9998
  9999
 10000


julia> z[1:5,1:10]
5×10 Array{Int32,2}:
 1   2   3   4   5   6   7   8   9  10
 2  42  42  42  42  42  42  42  42  42
 3  42  42  42  42  42  42  42  42  42
 4  42  42  42  42  42  42  42  42  42
 5  42  42  42  42  42  42  42  42  42
```

### Persistent arrays

In the examples above, compressed data for each chunk of the array was stored in main memory. Zarr arrays can also be stored on a file system, enabling persistence of data between sessions. For example:

```jldoctest persist
julia> using Zarr

julia> p = "data/example.zarr"
"data/example.zarr"

julia> z1 = zcreate(Int, 10000,10000,path = p,chunks=(1000, 1000))
ZArray{Int64} of size 10000 x 10000
```

The array above will store its configuration metadata and all compressed chunk data in a directory called ‘data/example.zarr’ relative to the current working directory. The zarr.create() function provides a way to create a new persistent array. Note that there is no need to close an array: data are automatically flushed to disk, and files are automatically closed whenever an array is modified.

Persistent arrays support the same interface for reading and writing data, e.g.:

```jldoctest persist
julia> z1[:] = 42
42

julia> z1[1,:]=1:10000;

julia> z1[:,1]=1:10000;

```

Check that the data have been written and can be read again:

```jldoctest persist
julia> z2 = zopen(p)
ZArray{Int64} of size 10000 x 10000

julia> all(z1[:,:].==z2[:,:])
true
```

*A Julia-equivalent for zarr.load and zarr.save is still missing...*

### Resizing and appending

*TODO*

### Compressors

A number of different compressors can be used with Zarr. In this Julia package we currently support only Blosc compression, but more compression methods will be supported in the future. Different compressors can be provided via the compressor keyword argument accepted by all array creation functions. For example:

```jldoctest compress
julia> using Zarr

julia> compressor = Zarr.BloscCompressor(cname="zstd", clevel=3, shuffle=true)
Zarr.BloscCompressor(0, 3, "zstd", true)

julia> data = Int32(1):Int32(1000000000)
1:1000000000

julia> z = Zarr.zcreate(Int32,10000, 10000, chunks = (1000,1000),compressor=compressor)
ZArray{Int32} of size 10000 x 10000

julia> z[:,:]=data
1:1000000000
```

This array above will use Blosc as the primary compressor, using the Zstandard algorithm (compression level 3) internally within Blosc, and with the bit-shuffle filter applied.

When using a compressor, it can be useful to get some diagnostics on the compression ratio. `ZArrays` provide a `zinfo` function which can be used to print some diagnostics, e.g.:

```jldoctest compress
julia> zinfo(z)
Type                : ZArray
Data type           : Int32
Shape               : (10000, 10000)
Chunk Shape         : (1000, 1000)
Order               : C
Read-Only           : false
Compressor          : Zarr.BloscCompressor(0, 3, "zstd", true)
Store type          : Dictionary Storage
No. bytes           : 400000000
No. bytes stored    : 2406369
Storage ratio       : 166.2255456249644
Chunks initialized  : 100/100
```


If you don’t specify a compressor, by default Zarr uses the Blosc compressor. Blosc is generally very fast and can be configured in a variety of ways to improve the compression ratio for different types of data. Blosc is in fact a “meta-compressor”, which means that it can use a number of different compression algorithms internally to compress the data. Blosc also provides highly optimized implementations of byte- and bit-shuffle filters, which can improve compression ratios for some data.

To disable compression, set `compressor=Zarr.NoCompressor()` when creating an array, e.g.:

```jldoctest compress
julia> z = zzeros(Int32,100000000, chunks=(1000000,), compressor=Zarr.NoCompressor());

julia> storageratio(z)
1.0
```
