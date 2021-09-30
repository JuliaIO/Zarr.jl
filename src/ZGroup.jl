struct ZGroup{S<:AbstractStore}
    storage::S
    path::String
    arrays::Dict{String, ZArray}
    groups::Dict{String, ZGroup}
    attrs::Dict
    writeable::Bool
end

zname(g::ZGroup) = zname(g.path)

#Open an existing ZGroup
function ZGroup(s::T,mode="r",path="") where T <: AbstractStore
  arrays = Dict{String, ZArray}()
  groups = Dict{String, ZGroup}()

  for d in subdirs(s,path)
    dshort = split(d,'/')[end]
    m = zopen_noerr(s,mode,path=_concatpath(path,dshort))
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
function zopen_noerr(s::AbstractStore, mode="r"; consolidated = false, path="")
    consolidated && isinitialized(s,".zmetadata") && return zopen(ConsolidatedStore(s, path), mode, path=path)
    if is_zarray(s, path)
        return ZArray(s,mode,path)
    elseif is_zgroup(s,path)
        return ZGroup(s,mode,path)
    else
        return nothing
    end
end

function Base.show(io::IO, g::ZGroup)
    print(io, "ZarrGroup at ", g.storage)
    !isempty(g.arrays) && print(io, "\nVariables: ", map(i -> string(zname(i), " "), values(g.arrays))...)
    !isempty(g.groups) && print(io, "\nGroups: ", map(i -> string(zname(i), " "), values(g.groups))...)
    nothing
end
Base.haskey(g::ZGroup,k)= haskey(g.groups,k) || haskey(g.arrays,k)


function Base.getindex(g::ZGroup, k)
    if haskey(g.groups, k)
        return g.groups[k]
    elseif haskey(g.arrays, k)
       return g.arrays[k]
    else
       throw(KeyError("Zarr Dataset does not contain $k"))
    end
end

"""
    zopen(s::AbstractStore, mode="r"; consolidated = false, path = "")

Opens a zarr Array or Group at Store `s`. If `consolidated` is set to "true",
Zarr will search for a consolidated metadata field as created by the python zarr
`consolidate_metadata` function. This can substantially speed up metadata parsing
of large zarr groups.
"""
function zopen(s::AbstractStore, mode="r"; consolidated = false, path = "")
    # add interfaces to Stores later    
    r = zopen_noerr(s,mode, consolidated=consolidated, path=path)
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
    s[".zgroup"]=take!(b)
    writeattrs(s,path,attrs)
    ZGroup(s, path, Dict{String,ZArray}(), Dict{String,ZGroup}(), attrs,true)
end

zgroup(s::String;kwargs...)=zgroup(storefromstring(s, true)...;kwargs...)

"Create a subgroup of the group g"
function zgroup(g::ZGroup, name; attrs=Dict()) 
  g.writeable || throw(IOError("Zarr group is not writeable. Please re-open in write mode to create an array"))
  g.groups[name] = zgroup(g.storage,attrs=attrs,path=_concatpath(g.path,name))
end

"Create a new subarray of the group g"
function zcreate(::Type{T},g::ZGroup, name::String, addargs...; kwargs...) where T
  g.writeable || throw(IOError("Zarr group is not writeable. Please re-open in write mode to create an array"))

  z = zcreate(T, g.storage, addargs...; path = _concatpath(g.path,name), kwargs...)
  g.arrays[name] = z
  return z
end

HTTP.serve(s::Union{ZArray,ZGroup}, args...; kwargs...) = HTTP.serve(s.storage, s.path, args...; kwargs...)
function consolidate_metadata(z::Union{ZArray,ZGroup}) 
  z.writeable || throw(Base.IOError("Zarr group is not writeable. Please re-open in write mode to create an array",0))
  consolidate_metadata(z.storage,z.path)
end
