struct ZGroup{S<:AbstractStore}
    storage::S
    arrays::Dict{String, ZArray}
    groups::Dict{String, ZGroup}
    attrs::Dict
    writeable::Bool
end

zname(g::ZGroup) = zname(g.storage)

#Open an existing ZGroup
function ZGroup(s::T,mode="r") where T <: AbstractStore
  arrays = Dict{String, ZArray}()
  groups = Dict{String, ZGroup}()

  for d in subdirs(s)
    dshort = splitpath(d)[end]
    m = zopen_noerr(getsub(s,dshort),mode)
    if isa(m, ZArray)
      arrays[dshort] = m
    elseif isa(m, ZGroup)
      groups[dshort] = m
    end
  end
  attrs = getattrs(s)
  ZGroup(s, arrays, groups, attrs,mode=="w")
end

"""
    zopen_noerr(AbstractStore, mode = "r"; consolidated = false)

Works like `zopen` with the single difference that no error is thrown when 
the path or store does not point to a valid zarr array or group, but nothing 
is returned instead. 
"""
function zopen_noerr(s::AbstractStore, mode="r"; consolidated = false)
    consolidated && isinitialized(s,".zmetadata") && return zopen(ConsolidatedStore(s), mode)
    if is_zarray(s)
        return ZArray(s,mode)
    elseif is_zgroup(s)
        return ZGroup(s,mode)
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
    zopen(s::AbstractStore, mode="r"; consolidated = false)

Opens a zarr Array or Group at Store `s`. If `consolidated` is set to "true",
Zarr will search for a consolidated metadata field as created by the python zarr
`consolidate_metadata` function. This can substantially speed up metadata parsing
of large zarr groups.
"""
function zopen(s::AbstractStore, mode="r"; consolidated = false)
    # add interfaces to Stores later    
    r = zopen_noerr(s,mode, consolidated=consolidated)
    if r === nothing
        x = path(s)
        throw(ArgumentError("Specified store ($x) is neither a ZArray nor a ZGroup"))
    else
        return r
    end
end

"""
    zopen(p::String, mode="r")

Open a zarr Array or group at disc path p.
"""
function zopen(s::String, mode="r"; kwargs...)
  zopen(storefromstring(s), mode; kwargs...)
end

function storefromstring(s)
  for (r,t) in storageregexlist
    if match(r,s) !== nothing
      return storefromstring(t,s)
    end
  end
  DirectoryStore(s)
end

"""
    zgroup(s::AbstractStore; attrs=Dict())

Create a new zgroup in the store `s`
"""
function zgroup(s::AbstractStore; attrs=Dict())
    d = Dict("zarr_format"=>2)
    isempty(s) || error("Store is not empty")
    b = IOBuffer()
    JSON.print(b,d)
    s[".zgroup"]=take!(b)
    writeattrs(s,attrs)
    ZGroup(s, Dict{String,ZArray}(), Dict{String,ZGroup}(), attrs,true)
end

zgroup(s::String;kwargs...)=zgroup(storefromstring(s);kwargs...)

"Create a subgroup of the group g"
function zgroup(g::ZGroup, name; attrs=Dict()) 
  g.writeable || throw(IOError("Zarr group is not writeable. Please re-open in write mode to create an array"))
  g.groups[name] = zgroup(newsub(g.storage,name),attrs=attrs)
end

"Create a new subarray of the group g"
function zcreate(::Type{T},g::ZGroup, name::String, addargs...; kwargs...) where T
  g.writeable || throw(IOError("Zarr group is not writeable. Please re-open in write mode to create an array"))
  newstore = newsub(g.storage,name)
  z = zcreate(T, newstore, addargs...; kwargs...)
  g.arrays[name] = z
  return z
end

HTTP.serve(s::Union{ZArray,ZGroup}, args...; kwargs...) = HTTP.serve(s.storage, args...; kwargs...)
function consolidate_metadata(z::Union{ZArray,ZGroup}) 
  z.writeable || throw(IOError("Zarr group is not writeable. Please re-open in write mode to create an array"))
  consolidate_metadata(z.storage)
end
