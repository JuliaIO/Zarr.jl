# I am still not sure LRUStore should actually a store or live 
# somewhere else. We are actually not extending the storage interface
# because we cache uncompressed chunks of data, which is already ones
# step further down the processing pipeline. Maybe there should rather
# be a mini-interface for the step of copying uncompressed data into
# the resulting array. However, let's try how this implementation
# works out and modify later if necessary
# TODO: add writing of data

using LRUCache: LRU
struct LRUStore{S<:AbstractStore} <: AbstractStore
    parent::S
    lru::LRU{String,Any}
end
function LRUStore(s::AbstractStore; maxsize=5)
    lru = LRU{String,Any}(maxsize=maxsize)
    LRUStore(s,lru)
end
storagesize(d::LRUStore,p) = storagesize(d.parent,p)
Base.getindex(d::LRUStore,i::AbstractString) = d.parent[i]
Base.setindex!(d::LRUStore,v,i::AbstractString) = error("Writing to LRU stores is currently not supported")
Base.delete!(d::LRUStore, i::AbstractString) = error("Writing to LRU stores is currently not supported")
subdirs(d::LRUStore,p) = subdirs(d.parent,p)
#subkeys(d::LRUStore,p) = subkeys(d.parent,p)

function readchunk!(a::DenseArray,z::ZArray{<:Any,N,<:Zarr.Compressor,<:LRUStore},i::CartesianIndex{N}) where N
    length(a) == prod(z.metadata.chunks) || throw(DimensionMismatch("Array size does not equal chunk size"))
    k = _concatpath(z.path,citostring(i,z.metadata.order==='C'))
    if haskey(z.storage.lru,k)
        a .= z.storage.lru[k]
        return a
    end 
    curchunk = z.storage[z.path,i]
    if curchunk === nothing
        fill!(a, z.metadata.fill_value)
    else
        zuncompress!(a, curchunk, z.metadata.compressor, z.metadata.filters)
    end
    z.storage.lru[k] = a
    a
end