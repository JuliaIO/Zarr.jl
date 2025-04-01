# Default Zarr version
const DV = 2

# Default Zarr separator

# Default Zarr v2 separator
const DS2 = '.'
# Default Zarr v3 separator
const DS3 = '/'

default_sep(version) = version == 2 ? DS2 :
                       version == 3 ? DS3 :
                       error("Unknown version: $version")
const DS = default_sep(DV)

# Chunk Key Encodings for Zarr v3
# A Char is the separator for the default chunk key encoding
struct V2ChunkKeyEncoding{SEP} end

# Version store differentiates between Zarr format versions
struct VersionedStore{V,SEP,STORE <: AbstractStore} <: AbstractStore
    parent::STORE
end
VersionedStore(args...) = VersionedStore{DV,DS}(args...)
VersionedStore(s::VersionedStore) = s
VersionedStore{V}(args...) where V = VersionedStore{V, default_sep(V)}(args...)
VersionedStore{V}(s::VersionedStore{<:Any,S}) where {V,S} = VersionedStore{V, S}(s)
VersionedStore{<: Any, S}(args...) where S = VersionedStore{DV, S}(args...)
VersionedStore{<: Any, S}(s::VersionedStore{V}) where {V,S} = VersionedStore{V, S}(s)
function VersionedStore{V,S}(store::AbstractStore) where {V,S}
    return VersionedStore{V,S,typeof(store)}(store)
end
function VersionedStore{V,S}(store::VersionedStore) where {V,S}
    p = parent(store)
    return VersionedStore{V,S,typeof(p)}(p)
end

Base.parent(store::VersionedStore) = store.parent

@inline citostring(i::CartesianIndex, version::Int, sep::Char=default_sep(version)) = (version == 3 ? "c$sep" : "" ) * join(reverse((i - oneunit(i)).I), sep)
@inline citostring(::CartesianIndex{0}, version::Int, sep::Char=default_sep(version)) = (version == 3 ? "c$(sep)0" : "0" )
@inline citostring(i::CartesianIndex, ::Int, ::Type{V2ChunkKeyEncoding{S}}) where S = citostring(i, 2, S)
citostring(i::CartesianIndex, s::VersionedStore{V, S}) where {V,S} = citostring(i, V, S)

Base.getindex(s::VersionedStore, p, i::CartesianIndex) = s[p, citostring(i,s)]
Base.delete!(s::VersionedStore, p, i::CartesianIndex) = delete!(s, p, citostring(i,s))
Base.setindex!(s::VersionedStore, v, p, i::CartesianIndex) = s[p, citostring(i,s)]=v

isinitialized(s::VersionedStore, p, i::CartesianIndex) = isinitialized(s,p,citostring(i, s))

"""
- [`storagesize(d::AbstractStore, p::AbstractString)`](@ref storagesize)
- [`subdirs(d::AbstractStore, p::AbstractString)`](@ref subdirs)
- [`subkeys(d::AbstractStore, p::AbstractString)`](@ref subkeys)
- [`isinitialized(d::AbstractStore, p::AbstractString)`](@ref isinitialized)
- [`storefromstring(::Type{<: AbstractStore}, s, _)`](@ref storefromstring)
- `Base.getindex(d::AbstractStore, i::AbstractString)`: return the data stored in key `i` as a Vector{UInt8}
- `Base.setindex!(d::AbstractStore, v, i::AbstractString)`: write the values in `v` to the key `i` of the given store `d`
"""

storagesize(d::VersionedStore, p::AbstractString) = storagesize(parent(d), p)
subdirs(d::VersionedStore, p::AbstractString) = subdirs(parent(d), p)
subkeys(d::VersionedStore, p::AbstractString) = subkeys(parent(d), p)
isinitialized(d::VersionedStore, p::AbstractString) = isinitialized(parent(d), p)
storefromstring(::Type{VersionedStore{<: Any, <: Any, STORE}}, s, _) where STORE = VersionedStore{DV,DS}(storefromstring(STORE, s))
storefromstring(::Type{VersionedStore{V,S}}, s, _) where {V,S} = VersionedStore{DV,DS}(storefromstring(s))
storefromstring(::Type{VersionedStore{V,S,STORE}}, s, _) where {V,S,STORE} = VersionedStore{V,S,STORE}(storefromstring(STORE, s))
Base.getindex(d::VersionedStore, i::AbstractString) = getindex(parent(d), i)
Base.setindex!(d::VersionedStore, v, i::AbstractString) = setindex!(parent(d), v, i)
Base.delete!(d::VersionedStore, i::AbstractString) = delete!(parent(d), i)


function Base.getproperty(store::VersionedStore{V,S}, sym::Symbol) where {V,S}
    if sym == :dimension_separator
        return S
    elseif sym == :zarr_format
        return V
    elseif sym ∈ propertynames(getfield(store, :parent))
        # Support forwarding of properties to parent
        return getproperty(store.parent, sym)
    else
        getfield(store, sym)
    end
end
function Base.propertynames(store::VersionedStore)
    return (:dimension_separator, :zarr_format, fieldnames(typeof(store))..., propertynames(store.parent)...)
end


"""
    Zarr.set_dimension_separator(store::VersionedStore{V}, sep::Char)::VersionedStore{V,sep}

Returns a VersionedStore of the same type with the same `zarr_format` parameter, `V`,
but with a dimension separator of `sep`. Note that this does not mutate the original store.

# Examples

```
julia> Zarr.set_dimension_separator(Zarr.VersionedStore{2, '.'}(Zarr.DictStore(), '/')) |> typeof
Zarr.VersionedStore{2, '/',Zarr.DictStore}
```
 
"""
function set_dimension_separator(store::VersionedStore{V}, sep::Char) where V
    return VersionedStore{V,sep}(store)
end
function set_dimension_separator(store::AbstractStore, sep::Char)
    return VersionedStore{<: Any,sep}(store)
end

"""
    set_zarr_format(::VersionedStore{<: Any, S}, zarr_format::Int)::VersionedStore{zarr_format,S}

Returns a VersionedStore of the same type with the same `dimension_separator` parameter, `S`,
but with the specified `zarr_format` parameter. Note that this does not mutate the original store.

# Examples

```
julia> Zarr.set_zarr_format(Zarr.VersionedStore{2, '.'}(Zarr.DictStore(), 3)) |> typeof
Zarr.VersionedStore{3, '.', DictStore}
```

"""
function set_zarr_format(store::VersionedStore{<: Any, S}, zarr_format::Int) where S
    return VersionedStore{zarr_format,S}(store)
end
function set_zarr_format(store::AbstractStore, zarr_format::Int)
    return VersionedStore{zarr_format}(store)
end

dimension_separator(::AbstractStore) = DS
dimension_separator(::VersionedStore{<: Any,S}) where S = S
zarr_format(::AbstractStore) = DV
zarr_format(::VersionedStore{V}) where V = V

is_zgroup(s::VersionedStore{3}, p, metadata=getmetadata(s, p, false)) =
    isinitialized(s,_concatpath(p,"zarr.json")) &&
    metadata.node_type == "group"
is_zarray(s::VersionedStore{3}, p, metadata=getmetadata(s, p, false)) =
    isinitialized(s,_concatpath(p,"zarr.json")) &&
    metadata.node_type == "array"

getmetadata(s::VersionedStore{3}, p,fill_as_missing) = Metadata(String(maybecopy(s[p,"zarr.json"])),fill_as_missing)
