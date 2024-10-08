struct ZGroup{S<:AbstractStore}
    storage::S
    path::String
    attrs::Dict
    writeable::Bool
    fill_as_missing::Bool
end

# path can also be a SubString{String}
ZGroup(storage, path::AbstractString, attrs, writeable, fill_as_missing) =
    ZGroup(storage, String(path), attrs, writeable, fill_as_missing)

zname(g::ZGroup) = zname(g.path)

#Open an existing ZGroup
function ZGroup(s::T,mode="r",path="";fill_as_missing=false) where T <: AbstractStore
    attrs = getattrs(s,path)
    startswith(path,"/") && error("Paths should never start with a leading '/'")
    ZGroup(s, path, attrs, mode == "w", fill_as_missing)
end

"""
    zopen_noerr(AbstractStore, mode = "r"; consolidated = false)

Works like `zopen` with the single difference that no error is thrown when 
the path or store does not point to a valid zarr array or group, but nothing 
is returned instead. 
"""
function zopen_noerr(s::AbstractStore, mode="r"; 
  consolidated = false, 
  path="", 
  lru = 0,
  fill_as_missing)
    consolidated && isinitialized(s,".zmetadata") && return zopen(ConsolidatedStore(s, path), mode, path=path,lru=lru,fill_as_missing=fill_as_missing)
    if lru !== 0 
      error("LRU caches are not supported anymore by the current Zarr version. Please use an earlier version of Zarr for now and open an issue at Zarr.jl if you need this functionality")
    end
    if is_zarray(s, path)
        return ZArray(s,mode,path;fill_as_missing=fill_as_missing)
    elseif is_zgroup(s,path)
        return ZGroup(s,mode,path;fill_as_missing=fill_as_missing)
    else
        return nothing
    end
end

function Base.show(io::IO, g::ZGroup)
    print(io, "ZarrGroup at ", g.storage, " and path ", g.path)
    for (i, d) in enumerate(subdirs(g.storage, g.path))
        if i > 10  # don't print too many
            print(io, "\n  ...")
            break
        end
        path = _concatpath(g.path, d)
        if is_zarray(g.storage, path)
            print(io, "\n  ", d, " (Array)")
        elseif is_zgroup(g.storage, path)
            print(io, "\n  ", d, " (Group)")
        end
    end
end

function Base.haskey(g::ZGroup, k)
    path = _concatpath(g.path, k)
    is_zarray(g.storage, path) || is_zgroup(g.storage, path)
end

function Base.getindex(g::ZGroup, k)
    m = zopen_noerr(g.storage, g.writeable ? "w" : "r", path = _concatpath(g.path, k), fill_as_missing = g.fill_as_missing)
    m !== nothing ? m : throw(KeyError(k))
end

"""
    zopen(s::AbstractStore, mode="r"; consolidated = false, path = "", lru = 0)

Opens a zarr Array or Group at Store `s`. If `consolidated` is set to "true",
Zarr will search for a consolidated metadata field as created by the python zarr
`consolidate_metadata` function. This can substantially speed up metadata parsing
of large zarr groups. Setting `lru` to a value > 0 means that chunks that have been
accessed before will be cached and consecutive reads will happen from the cache. 
Here, `lru` denotes the number of chunks that remain in memory. 
"""
function zopen(s::AbstractStore, mode="r"; 
  consolidated = false, 
  path = "", 
  lru = 0,
  fill_as_missing = false)
    # add interfaces to Stores later    
    r = zopen_noerr(s,mode; consolidated=consolidated, path=path, lru=lru, fill_as_missing=fill_as_missing)
    if r === nothing
        throw(ArgumentError("Specified store $s in path $(path) is neither a ZArray nor a ZGroup"))
    else
        return r
    end
end

"""
    zopen(p::String, mode="r")

Open a zarr Array or group at disc path p.
"""
function zopen(s::String, mode="r"; kwargs...)
  store, path = storefromstring(s,false)
  zopen(store, mode; path=path, kwargs...)
end

function storefromstring(s, create=true)
  for (r,t) in storageregexlist
    if match(r,s) !== nothing
      return storefromstring(t,s,create)
    end
  end
  if create || isdir(s)
    return DirectoryStore(s), ""
  else
    throw(ArgumentError("Path $s is not a directory."))
  end
end

"""
    zgroup(s::AbstractStore; attrs=Dict())

Create a new zgroup in the store `s`
"""
function zgroup(s::AbstractStore, path::String=""; attrs=Dict())
    d = Dict("zarr_format"=>2)
    isemptysub(s, path) || error("Store is not empty")
    b = IOBuffer()
    JSON.print(b,d)
    s[path,".zgroup"]=take!(b)
    writeattrs(s,path,attrs)
    ZGroup(s, path, attrs, true, false)
end

zgroup(s::String;kwargs...)=zgroup(storefromstring(s, true)...;kwargs...)

"Create a subgroup of the group g"
function zgroup(g::ZGroup, name; attrs=Dict()) 
  g.writeable || throw(IOError("Zarr group is not writeable. Please re-open in write mode to create an array"))
  zgroup(g.storage, _concatpath(g.path, name), attrs = attrs)
end

"Create a new subarray of the group g"
function zcreate(::Type{T},g::ZGroup, name::AbstractString, addargs...; kwargs...) where T
  g.writeable || throw(IOError("Zarr group is not writeable. Please re-open in write mode to create an array"))
  name = string(name)
  zcreate(T, g.storage, addargs...; path = _concatpath(g.path, name), kwargs...)
end

HTTP.serve(s::Union{ZArray,ZGroup}, args...; kwargs...) = HTTP.serve(s.storage, s.path, args...; kwargs...)
writezip(io::IO, s::Union{ZArray,ZGroup}; kwargs...) = writezip(io, s.storage, s.path; kwargs...)
function consolidate_metadata(z::Union{ZArray,ZGroup}) 
  z.writeable || throw(Base.IOError("Zarr group is not writeable. Please re-open in write mode to create an array",0))
  consolidate_metadata(z.storage,z.path)
end
