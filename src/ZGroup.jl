struct ZGroup{S<:AbstractStore}
    storage::S
    arrays::Dict{String, ZArray}
    groups::Dict{String, ZGroup}
    attrs::Dict
end

zname(g::ZGroup) = zname(g.storage)

function ZGroup(s::T,mode="r") where T <: AbstractStore
    arrays = Dict{String, ZArray}()
    groups = Dict{String, ZGroup}()

    for d in subs(s)
        m = zopen(T(s, d),mode)
        if isa(m, ZArray)
            arrays[d] = m
        else
            groups[d] = m
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

function Base.getindex(g::ZGroup, k)
    if haskey(g.groups, k)
        return g.groups[k]
    elseif haskey(g.arrays, k)
       return g.arrays[k]
    else
       throw(KeyError("Zarr Dataset does not contain ", k))
    end
end

function zopen(s::T, mode="r") where T <: AbstractStore
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


function zgroup(p::String; attrs=Dict())
    d = Dict("zarr_format"=>2)
    isdir(p) && throw(ArgumentError("Path $p already exists."))
    mkpath(p)
    open(joinpath(p, ".zgroup"), "w") do f
       JSON.print(f, d)
    end
    ZGroup(DirectoryStore(p), Dict{String,ZArray}(), Dict{String,ZGroup}(), attrs)
end

"Create a subgroup of the group g"
function zgroup(g::ZGroup, name; attrs=Dict())
  if isa(g.storage, DictStore)
      error("Not implemented")
  elseif isa(g.storage, DirectoryStore)
      zgroup(joinpath(g.storage.folder, "name"), attrs=attrs)
  end
end

"Create a new subarray of the group g"
function zcreate(g::ZGroup, name::String, addargs...; kwargs...)
    if isa(g.storage, DictStore)
        error("Not implemented")
    elseif isa(g.storage, DirectoryStore)
        z = zcreate(addargs...; kwargs..., name = name, path=g.storage.folder)
        g.arrays[name] = z
        z
    end
end
