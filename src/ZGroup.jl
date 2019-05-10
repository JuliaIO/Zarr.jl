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
    zopen(s::AbstractStore, mode="r")

Opens a zarr Array or Group at Store `s`
"""
function zopen(s::AbstractStore, mode="r")
    # add interfaces to Stores later
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
function zopen(s::String, mode="r")
  #TODO this could include some heuristics to determine if this is a local
  #Directory or a s3 path or a zip file... Currently we assuem a local store
  zopen(DirectoryStore(s), mode)
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
  zcreate(T, newstore, addargs...; kwargs...)
end
