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
abstract type ChunkKeyEncoding end
struct V2ChunkKeyEncoding{SEP} <: ChunkKeyEncoding end
separator(c::Char) = c
separator(v2cke::V2ChunkKeyEncoding{SEP}) where SEP = SEP

"""
    FormattedStore{V,CKE,STORE <: AbstractStore} <: AbstractStore

FormattedStore wraps an AbstractStore to indicate a specific Zarr format.
The path of a chunk depends on the version and chunk key encoding.

# Type Parameters

- V: Zarr format version
- CKE: Chunk key encoding or dimension separator.
       CKE could be a `Char` or a subtype of `ChunkKeyEncoding`.
- STORE: Type of AbstractStore wrapped

# Chunk Path Formats

## Zarr version 2

### '.' dimension separator (default)

Chunks are encoded as "1.2.3"

### '/' dimension separator

Chunks are encoded as "1/2/3"

## Zarr version 3

### '/' dimension separator (default)

Chunks are encoded as "c/1/2/3"

### '.' dimension separator

Chunks are encoded as "c.1.2.3"

### V2ChunkKeyEncoding{SEP}

See Zarr version 2
"""
struct FormattedStore{V,SEP,STORE <: AbstractStore} <: AbstractStore
    parent::STORE
end
FormattedStore(args...) = FormattedStore{DV,DS}(args...)
FormattedStore(s::FormattedStore) = s
FormattedStore{V}(args...) where V = FormattedStore{V, default_sep(V)}(args...)
FormattedStore{V}(s::FormattedStore{<:Any,S}) where {V,S} = FormattedStore{V, S}(s)
FormattedStore{<: Any, S}(args...) where S = FormattedStore{DV, S}(args...)
FormattedStore{<: Any, S}(s::FormattedStore{V}) where {V,S} = FormattedStore{V, S}(s)
function FormattedStore{V,S}(store::AbstractStore) where {V,S}
    return FormattedStore{V,S,typeof(store)}(store)
end
function FormattedStore{V,S}(store::FormattedStore) where {V,S}
    p = parent(store)
    return FormattedStore{V,S,typeof(p)}(p)
end

Base.parent(store::FormattedStore) = store.parent

@inline citostring(i::CartesianIndex, version::Int, sep::Char=default_sep(version)) = (version == 3 ? "c$sep" : "" ) * join(reverse((i - oneunit(i)).I), sep)
@inline citostring(::CartesianIndex{0}, version::Int, sep::Char=default_sep(version)) = (version == 3 ? "c$(sep)0" : "0" )
@inline citostring(i::CartesianIndex, ::Int, ::Type{V2ChunkKeyEncoding{S}}) where S = citostring(i, 2, S)
citostring(i::CartesianIndex, s::FormattedStore{V, S}) where {V,S} = citostring(i, V, S)

Base.getindex(s::FormattedStore, p, i::CartesianIndex) = s[p, citostring(i,s)]
Base.delete!(s::FormattedStore, p, i::CartesianIndex) = delete!(s, p, citostring(i,s))
Base.setindex!(s::FormattedStore, v, p, i::CartesianIndex) = s[p, citostring(i,s)]=v

isinitialized(s::FormattedStore, p, i::CartesianIndex) = isinitialized(s,p,citostring(i, s))

"""
- [`storagesize(d::AbstractStore, p::AbstractString)`](@ref storagesize)
- [`subdirs(d::AbstractStore, p::AbstractString)`](@ref subdirs)
- [`subkeys(d::AbstractStore, p::AbstractString)`](@ref subkeys)
- [`isinitialized(d::AbstractStore, p::AbstractString)`](@ref isinitialized)
- [`storefromstring(::Type{<: AbstractStore}, s, _)`](@ref storefromstring)
- `Base.getindex(d::AbstractStore, i::AbstractString)`: return the data stored in key `i` as a Vector{UInt8}
- `Base.setindex!(d::AbstractStore, v, i::AbstractString)`: write the values in `v` to the key `i` of the given store `d`
"""

storagesize(d::FormattedStore, p::AbstractString) = storagesize(parent(d), p)
subdirs(d::FormattedStore, p::AbstractString) = subdirs(parent(d), p)
subkeys(d::FormattedStore, p::AbstractString) = subkeys(parent(d), p)
isinitialized(d::FormattedStore, p::AbstractString) = isinitialized(parent(d), p)
storefromstring(::Type{FormattedStore{<: Any, <: Any, STORE}}, s, _) where STORE = FormattedStore{DV,DS}(storefromstring(STORE, s))
storefromstring(::Type{FormattedStore{V,S}}, s, _) where {V,S} = FormattedStore{DV,DS}(storefromstring(s))
storefromstring(::Type{FormattedStore{V,S,STORE}}, s, _) where {V,S,STORE} = FormattedStore{V,S,STORE}(storefromstring(STORE, s))
Base.getindex(d::FormattedStore, i::AbstractString) = getindex(parent(d), i)
Base.setindex!(d::FormattedStore, v, i::AbstractString) = setindex!(parent(d), v, i)
Base.delete!(d::FormattedStore, i::AbstractString) = delete!(parent(d), i)


function Base.getproperty(store::FormattedStore{V,S}, sym::Symbol) where {V,S}
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
function Base.propertynames(store::FormattedStore)
    return (:dimension_separator, :zarr_format, fieldnames(typeof(store))..., propertynames(store.parent)...)
end


"""
    Zarr.set_dimension_separator(store::FormattedStore{V}, sep::Char)::FormattedStore{V,sep}

Returns a FormattedStore of the same type with the same `zarr_format` parameter, `V`,
but with a dimension separator of `sep`. Note that this does not mutate the original store.

# Examples

```
julia> Zarr.set_dimension_separator(Zarr.FormattedStore{2, '.'}(Zarr.DictStore(), '/')) |> typeof
Zarr.FormattedStore{2, '/',Zarr.DictStore}
```
 
"""
function set_dimension_separator(store::FormattedStore{V}, sep::Char) where V
    return FormattedStore{V,sep}(store)
end
function set_dimension_separator(store::AbstractStore, sep::Char)
    return FormattedStore{<: Any,sep}(store)
end

"""
    set_zarr_format(::FormattedStore{<: Any, S}, zarr_format::Int)::FormattedStore{zarr_format,S}

Returns a FormattedStore of the same type with the same `dimension_separator` parameter, `S`,
but with the specified `zarr_format` parameter. Note that this does not mutate the original store.

# Examples

```
julia> Zarr.set_zarr_format(Zarr.FormattedStore{2, '.'}(Zarr.DictStore(), 3)) |> typeof
Zarr.FormattedStore{3, '.', DictStore}
```

"""
function set_zarr_format(store::FormattedStore{<: Any, S}, zarr_format::Int) where S
    return FormattedStore{zarr_format,S}(store)
end
function set_zarr_format(store::AbstractStore, zarr_format::Int)
    return FormattedStore{zarr_format}(store)
end

dimension_separator(::AbstractStore) = DS
dimension_separator(::FormattedStore{<: Any,S}) where S = S
zarr_format(::AbstractStore) = DV
zarr_format(::FormattedStore{V}) where V = V

is_zgroup(s::FormattedStore{3}, p, metadata=getmetadata(s, p, false)) =
    isinitialized(s,_concatpath(p,"zarr.json")) &&
    metadata.node_type == "group"
is_zarray(s::FormattedStore{3}, p, metadata=getmetadata(s, p, false)) =
    isinitialized(s,_concatpath(p,"zarr.json")) &&
    metadata.node_type == "array"

getmetadata(s::FormattedStore{3}, p,fill_as_missing) = Metadata(String(maybecopy(s[p,"zarr.json"])),fill_as_missing)

function getattrs(s::FormattedStore{3})
  md = s[p,"zarr.json"]
  if md === nothing
    error("zarr.json not found")
  else
    md = JSON.parse(replace(String(maybecopy(md)),": NaN,"=>": \"NaN\","))
    return get(md, "attributes", Dict{String, Any}())
  end
end

function writeattrs(s::FormattedStore{3}, p, att::Dict; indent_json::Bool= false)
  # This is messy, we need to open zarr.json and replace the attributes section
  md = s[p,"zarr.json"]
  if md === nothing
    error("zarr.json not found")
  else
    md = JSON.parse(replace(String(maybecopy(md)),": NaN,"=>": \"NaN\","))
  end
  md = Dict(md)
  md["attributes"] = att

  b = IOBuffer()

  if indent_json
    JSON.print(b,att,4)
  else
    JSON.print(b,att)
  end

  s[p,"zarr.json"] = take!(b)
  att
end
