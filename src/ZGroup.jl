struct ZGroup{S<:ZStorage}
    storage::S
    arrays::Dict{String, ZArray}
    groups::Dict{String, ZGroup}
    attrs::Dict
end

zname(g::ZGroup) = zname(g.storage)

function ZGroup(p::String)
    arrays = Dict{String, ZArray}()
    groups = Dict{String, ZGroup}()
    for d in filter(i -> isdir(joinpath(p, i)), readdir(p))
        m = zopen(joinpath(p, d))
        if isa(m, ZArray)
            arrays[d] = m
        else
            groups[d] = m
        end
    end
    attrs = getattrs(DiskStorage(p))
    ZGroup(DiskStorage(p), arrays, groups, attrs)
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

function zopen(p::String)
    if isfile(joinpath(p, ".zarray"))
        return ZArray(p)
    elseif isfile(joinpath(p, ".zgroup"))
        return ZGroup(p)
    else
        throw(ArgumentError("Specified path $p is neither a ZArray nor a ZGroup"))
    end
end

function zgroup(p::String; attrs=Dict())
    d = Dict("zarr_format"=>2)
    isdir(p) && throw(ArgumentError("Path $p already exists."))
    mkpath(p)
    open(joinpath(p, ".zgroup"), "w") do f
       JSON.print(f, d)
    end
    ZGroup(DiskStorage(p), Dict{String,ZArray}(), Dict{String,ZGroup}(), attrs)
end

function zzeros(g::ZGroup, addargs...; kwargs...)
    :name in keys(kwargs) || throw(ArgumentError("You must provide a name"))
    if isa(g.storage, MemStorage)
        error("Not implemented")
    elseif isa(g.storage, DiskStorage)
        zzeros(addargs...; kwargs..., path=joinpath(g.storage.folder, "name"))
    end
end
