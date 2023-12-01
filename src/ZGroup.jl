struct ZGroup{S<:AbstractStore}
    storage::S
    path::String
    arrays::Dict{String, ZArray}
    groups::Dict{String, ZGroup}
    attrs::Dict
    writeable::Bool
end
const ZArrayOrGroup = Union{ZArray, ZGroup}
storage(a::ZArrayOrGroup)=getfield(a,:storage)
path(a::ZArrayOrGroup)=getfield(a,:path)
attributes(a::ZArrayOrGroup)=getfield(a,:attrs)
iswriteable(a::ZArrayOrGroup)=getfield(a,:writeable)
arrays(g::ZGroup)=getfield(g,:arrays)
groups(g::ZGroup)=getfield(g,:groups)
export attributes

# path can also be a SubString{String}
ZGroup(storage, path::AbstractString, arrays, groups, attrs, writeable) =
    ZGroup(storage, String(path), arrays, groups, attrs, writeable)

zname(g::ZGroup) = zname(path(g))

#Open an existing ZGroup
function ZGroup(s::T,mode="r",path="";fill_as_missing=false) where T <: AbstractStore
  arrays = Dict{String, ZArray}()
  groups = Dict{String, ZGroup}()

  for d in subdirs(s,path)
    dshort = split(d,'/')[end]
    m = zopen_noerr(s,mode,path=_concatpath(path,dshort),fill_as_missing=fill_as_missing)
    if isa(m, ZArray)
      arrays[dshort] = m
    elseif isa(m, ZGroup)
      groups[dshort] = m
    end
  end
  attrs = getattrs(s,path)
  startswith(path,"/") && error("Paths should never start with a leading '/'")
  ZGroup(s, path, arrays, groups, attrs,mode=="w")
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
    print(io, "ZarrGroup at ", storage(g), " and path ", path(g))
    !isempty(arrays(g)) && print(io, "\nVariables: ", map(i -> string(zname(i), " "), values(arrays(g)))...)
    !isempty(groups(g)) && print(io, "\nGroups: ", map(i -> string(zname(i), " "), values(groups(g)))...)
    nothing
end
Base.haskey(g::ZGroup,k)= haskey(groups(g),string(k)) || haskey(arrays(g),string(k))

function Base.getindex(g::ZGroup, k::AbstractString)
    if haskey(groups(g), k)
        return groups(g)[k]
    elseif haskey(arrays(g), k)
       return arrays(g)[k]
    else
       throw(KeyError("Zarr Dataset does not contain $k"))
    end
end
Base.getindex(g::ZGroup,k)=getindex(g,string(k))
function Base.propertynames(g::ZGroup,private::Bool=false)
  p = if private
    Symbol[:attrs]
  else
    Symbol[]
  end
  for k in keys(groups(g))
    push!(p,Symbol(k))
  end
  for k in keys(arrays(g))
    push!(p,Symbol(k))
  end
  p
end

function Base.getproperty(g::ZGroup, k::Symbol)
  if k === :attrs
    @warn "Accessing attributes through `.attrs` is not recommended anymore. Please use `attributes(g)` instead."
    return getfield(g,:attrs)
  else
    return g[k]
  end
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
    ZGroup(s, path, Dict{String,ZArray}(), Dict{String,ZGroup}(), attrs,true)
end

zgroup(s::String;kwargs...)=zgroup(storefromstring(s, true)...;kwargs...)

"Create a subgroup of the group g"
function zgroup(g::ZGroup, name; attrs=Dict()) 
  iswriteable(g) || throw(IOError("Zarr group is not writeable. Please re-open in write mode to create an array"))
  groups(g)[name] = zgroup(storage(g),_concatpath(path(g),name),attrs=attrs)
end

"Create a new subarray of the group g"
function zcreate(::Type{T},g::ZGroup, name::AbstractString, addargs...; kwargs...) where T
  iswriteable(g) || throw(IOError("Zarr group is not writeable. Please re-open in write mode to create an array"))
  name = string(name)
  z = zcreate(T, storage(g), addargs...; path = _concatpath(path(g),name), kwargs...)
  arrays(g)[name] = z
  return z
end

HTTP.serve(s::Union{ZArray,ZGroup}, args...; kwargs...) = HTTP.serve(storage(s), path(s), args...; kwargs...)
function consolidate_metadata(z::Union{ZArray,ZGroup}) 
  iswriteable(z) || throw(Base.IOError("Zarr group is not writeable. Please re-open in write mode to create an array",0))
  consolidate_metadata(storage(z),path(z))
end
