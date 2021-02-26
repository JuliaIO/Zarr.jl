struct ZGroup{S<:AbstractStore}
    storage::S
    arrays::Dict{String, ZArray}
    groups::Dict{String, ZGroup}
    attrs::Dict
end

zname(g::ZGroup) = zname(g.storage)

#Open an existing ZGroup
function ZGroup(s::T,mode="r") where T <: AbstractStore
  arrays = Dict{String, ZArray}()
  groups = Dict{String, ZGroup}()

  for d in subdirs(s)
    dshort = splitpath(d)[end]
    m = zopen(getsub(s,dshort),mode)
    if isa(m, ZArray)
      arrays[dshort] = m
    else
      groups[dshort] = m
    end
  end
  attrs = getattrs(s)
  ZGroup(s, arrays, groups, attrs)
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
    consolidated && isinitialized(s,".zmetadata") && return zopen(ConsolidatedStore(s), mode)
    if is_zarray(s)
        return ZArray(s,mode)
    elseif is_zgroup(s)
        return ZGroup(s,mode)
    else
        x = path(s)
        throw(ArgumentError("Specified store ($x) is neither a ZArray nor a ZGroup"))
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
    ZGroup(s, Dict{String,ZArray}(), Dict{String,ZGroup}(), attrs)
end

zgroup(s::String;kwargs...)=zgroup(DirectoryStore(s);kwargs...)

"Create a subgroup of the group g"
zgroup(g::ZGroup, name; attrs=Dict()) = g.groups[name] = zgroup(newsub(g.storage,name),attrs=attrs)

"Create a new subarray of the group g"
function zcreate(::Type{T},g::ZGroup, name::String, addargs...; kwargs...) where T
  newstore = newsub(g.storage,name)
  z = zcreate(T, newstore, addargs...; kwargs...)
  g.arrays[name] = z
  return z
end

HTTP.serve(s::Union{ZArray,ZGroup}, args...; kwargs...) = HTTP.serve(s.storage, args...; kwargs...)
consolidate_metadata(z::Union{ZArray,ZGroup}) = consolidate_metadata(z.storage)
