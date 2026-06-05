# Locally caching data

## In-memory caching

Quite often one has to deal with remote data that is very slow to access. Some applications like interactive visualisation
depend on repeated access to slow data sources, where local caching can drastically improve the performance. 
For local in-memory lru caching we can use the `cache` method provided by DiskArrays.jl. Let's first create a large array to cache:

````jldoctest cache
julia> using Zarr, DiskArrays

julia> p=tempname();

julia> a = zcreate(Float64,10000,10000,path=p,chunks=(1000,1000),fill_value=NaN)
ZArray{Float64} of size 10000 x 10000

julia> a[1,:] = 1:10000;

julia> a[:,1] .= 1:10000;
````

and wrap it into a `CachedDiskArray`:

````jldoctest cache
julia> a_lrucache = DiskArrays.cache(a,maxsize=1)
10000×10000 DiskArrays.CachedDiskArray{Float64, 2, ZArray{Float64, 2, DirectoryStore, Zarr.MetadataV2{Float64, 2, Zarr.BloscCompressor, Nothing}}, LRUCache.LRU{ChunkIndex{2, DiskArrays.OffsetChunks}, OffsetArrays.OffsetMatrix{Float64, Matrix{Float64}}}}

Chunked: (
    [1000, 1000, 1000, 1000, 1000, 1000, 1000, 1000, 1000, 1000]
    [1000, 1000, 1000, 1000, 1000, 1000, 1000, 1000, 1000, 1000]
)

julia> a_lrucache[5000,1] # Precompilation
5000.0

julia> a_lrucache[5001,1]
5001.0
````

You will realize that accessing data from the same chunk multiple times will be much faster than the first access, because the data
is loaded from memory. 

## Persistent caching

Sometimes you want your cache to be persistent across sessions to avoid re-downloading data from remote sources which can be quite 
costly especially when accessing from s3 buckets. In Zarr.jl we provide 2 ways to locally store partially cached data. 

### Persistent array caching

Instead of using an in-memory LRU-cache as the caching layer for an array on can store cached chunks directly in a ZArray. 
Whenever data is accessed there will be a check if the corresponding chunk already exists on disk and it will be downloaded on
demand. 

So, if we want to hold an incomplete local copy of any `AbstractDiskArray` you can do

````jldoctest cache
julia> a_arraycache = zarrcache(a,"./my_persistent_store.zarr")
10000×10000 DiskArrays.CachedDiskArray{Float64, 2, ZArray{Float64, 2, DirectoryStore, Zarr.MetadataV2{Float64, 2, Zarr.BloscCompressor, Nothing}}, Zarr.PermanentZarrCache{Float64, 2, ZArray{Float64, 2, DirectoryStore, Zarr.MetadataV2{Float64, 2, Zarr.BloscCompressor, Nothing}}}}

Chunked: (
    [1000, 1000, 1000, 1000, 1000, 1000, 1000, 1000, 1000, 1000]
    [1000, 1000, 1000, 1000, 1000, 1000, 1000, 1000, 1000, 1000]
)
````

This is very similar to the lru example, but now every cached chunk will be written to disk. Rerunning this code in another session
will open the existing cache and not re-download any data from remote sources. 

Note also that the local array at `"./my_persistent_store.zarr"` is a completely function Zarr array, mirroring the attributes
of the remote array and can also be loaded from local source only:

````jldoctest cache
julia> zopen("./my_persistent_store.zarr/")
ZArray{Float64} of size 10000 x 10000
````

### Persistent store caching

The methods described above operated on the `AbstractDiskArray` abstraction. Therefore these methods can work on any DiskArray, not 
only for Zarr, but they can only cache array data and no metadata. A different caching strategy is therefore to cache arrays directly 
at the storage level, where there is a lookup only on key-value-pairs. Therefore, not only chunks but also metadata can be cached using this approach. 

````jldoctest cache
julia> caching_store = Zarr.CachingStore(p, "./my_other_persistent_store.zarr")
Caching Storage

julia> a_storecached = zopen(caching_store)
ZArray{Float64} of size 10000 x 10000
````

Now you can use a_storecached as a cached version of the original array. In this approach, only caching of compressed chunks
is possible as the cache is only a duplication of the storage layer, not its array interpretation. Also, it can only be used 
for Zarr-based arrays and groups and not for other DiskArrays.


