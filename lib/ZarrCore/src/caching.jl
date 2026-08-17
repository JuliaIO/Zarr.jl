import DiskArrays: approx_chunksize, eachchunk, CachedDiskArray, ChunkIndex
export zarrcache

struct PermanentZarrCache{T,N,A<:ZArray{T,N}} 
    a::A
end
function Base.get!(f, a::PermanentZarrCache, i::ChunkIndex)
    cikey = citostring(a.a.metadata.chunk_key_encoding, CartesianIndex(i.I))
    if isinitialized(a.a.storage, a.a.path, cikey)
        a.a[i]
    else
        data = f()
        if !isnothing(data)
            a.a[i] = parent(data)
        end
        data
    end
end
function PermanentZarrCache(a::AbstractArray, path::String; fill_value=nothing, attrs=Dict())
     # We create the cache array on disk if it does not already exist, otherwise we open it.
    fill_as_missing = eltype(a) >: Missing
    cache = if isdir(path)
        cache = zopen(path, "w"; fill_as_missing)
        size(cache) == size(a) || error("sizes do not match")
        eltype(cache) == eltype(a) || error("element type does not match")
        cache
    else
        zcreate(eltype(a), size(a)...; chunks=approx_chunksize(eachchunk(a)), path=path, fill_as_missing, fill_value, attrs)
    end
    PermanentZarrCache(cache)
end

# Some convenience methods that create a permanent cache including metadata entries
# so that the cache can be loaded directly if needed
"""
    zarrcache(a::AbstractArray, local_path; fill_value=nothing)

Create a permanent on-disk Zarr cache for array `a` at `local_path`.
If the path already exists, the cache is opened and validated against `a`.
If not, a new Zarr array is created with the same size and element type.
"""
zarrcache(a::AbstractArray, local_path;fill_value=nothing) = CachedDiskArray(a, PermanentZarrCache(a, local_path;fill_value), false)

"""
    zarrcache(a::ZArray, local_path; fill_value=a.metadata.fill_value, attrs=a.attrs)

Create a permanent cache for an existing Zarr array `a` at `local_path`.
The cached copy preserves Zarr metadata such as `fill_value` and `attrs`.
"""
zarrcache(a::ZArray, local_path; fill_value = a.metadata.fill_value, attrs=a.attrs) = CachedDiskArray(a, PermanentZarrCache(a, local_path;fill_value,attrs), false)

"""
    zarrcache(g::ZGroup, local_path)

Create a permanent cache for all arrays and subgroups in Zarr group `g`.
The result is a dictionary mapping array and group names to cached Zarr objects stored under `local_path`.
"""
function zarrcache(g::ZGroup, local_path)
    c = if ispath(local_path)
        zopen(local_path)
    else    
        zgroup(local_path, attrs=g.attrs)
    end
    cache_array_dict = Dict(name => zarrcache(g.arrays[name],joinpath(local_path,name)) for name in keys(g.arrays))
    cache_group_dict = Dict(name => zarrcache(g.groups[name],joinpath(local_path,name)) for name in keys(g.groups))
    merge(cache_array_dict, cache_group_dict)
end
        
