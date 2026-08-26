
# Defines different storages for zarr arrays. Currently only regular files (DirectoryStore)
# and Dictionaries are supported

"""
    abstract type AbstractStore 

This the abstract supertype for all Zarr store implementations.  Currently only regular files ([`DirectoryStore`](@ref))
and Dictionaries are supported.

## Interface

All subtypes of `AbstractStore` must implement the following methods:

- [`storagesize(d::AbstractStore, p::AbstractString)`](@ref storagesize)
- [`subdirs(d::AbstractStore, p::AbstractString)`](@ref subdirs)
- [`subkeys(d::AbstractStore, p::AbstractString)`](@ref subkeys)
- [`isinitialized(d::AbstractStore, p::AbstractString)`](@ref isinitialized)
- [`storefromstring(::Type{<: AbstractStore}, s, _)`](@ref storefromstring)
- `Base.getindex(d::AbstractStore, i::AbstractString)`: return the data stored in key `i` as a Vector{UInt8}
- `Base.setindex!(d::AbstractStore, v, i::AbstractString)`: write the values in `v` to the key `i` of the given store `d`

They may optionally implement the following methods:

- [`store_read_strategy(s::AbstractStore)`](@ref store_read_strategy): return the read strategy for the given store.  See [`SequentialRead`](@ref) and [`ConcurrentRead`](@ref).
"""
abstract type AbstractStore end

# Define the interface

"""
    storagesize(d::AbstractStore, p::AbstractString)

This function shall return the size of all data files in a store at path `p`.
"""
function storagesize end


"""
    Base.getindex(d::AbstractStore,i::String)

Returns the data stored in the given key as a Vector{UInt8}
"""
Base.getindex(d::AbstractStore,i::AbstractString) = error("getindex not implemented for store $(typeof(d))")

"""
    Base.setindex!(d::AbstractStore,v,i::String)

Writes the values in v to the given store and key.
"""
Base.setindex!(d::AbstractStore,v,i::AbstractString) = error("setindex not implemented for store $(typeof(d))")

"""
    subdirs(d::AbstractStore, p)

Returns a list of keys for children stores in the given store at path p.
"""
function subdirs end

"""
    subkeys(d::AbstractStore, p)

Returns the keys of files in the given store.
"""
function subkeys end

"""
    cloud_list_objects(s::AbstractStore, p)

Backend extension point for listing objects and prefixes below `p`.
"""
function cloud_list_objects end

# Function to construct the full path to a chunk given the base path, Cartesian Index i, and the chunk ecoding
store_readchunk(s::AbstractStore, p, i::CartesianIndex, e::AbstractChunkKeyEncoding) = s[p, citostring(e, i)]
store_deletechunk(s::AbstractStore, p, i::CartesianIndex, e::AbstractChunkKeyEncoding) = delete!(s, p, citostring(e, i))
store_writechunk(s::AbstractStore, v, p, i::CartesianIndex, e::AbstractChunkKeyEncoding) = s[p, citostring(e, i)] = v
store_isinitialized(s::AbstractStore, p, i::CartesianIndex, e::AbstractChunkKeyEncoding) = isinitialized(s, p, citostring(e, i))


#Functions to concat path and key 
Base.getindex(s::AbstractStore, p, i::AbstractString) = s[_concatpath(p, i)]
Base.delete!(s::AbstractStore, p, i::AbstractString) = delete!(s, _concatpath(p, i))
Base.haskey(s::AbstractStore, k::AbstractString) = isinitialized(s, k)
Base.setindex!(s::AbstractStore, v, p, i::AbstractString) = setindex!(s, v, _concatpath(p, i))


maybecopy(x) = copy(x)
maybecopy(x::String) = x


function getattrs(::ZarrFormat{2}, s::AbstractStore, p)
  atts = s[p,".zattrs"]
  if atts === nothing
    Dict()
  else
    JSON.parse(replace(String(maybecopy(atts)),": NaN,"=>": \"NaN\","); dicttype = Dict{String,Any})
  end
end

"""
    getattrs_typed(::ZarrFormat{2}, s::AbstractStore, p) -> Dict{String,Any}

Statically typed counterpart of `getattrs(::ZarrFormat{2}, ...)`: parses
`.zattrs` with an explicitly asserted concrete result type and copies it into a
`Dict{String,Any}` with a plain loop.

`JSON.parse(...; dicttype = Dict{String,Any})` (used by the dynamic method) is
not `--trim=safe`: the `dicttype` keyword widens to `DataType` and the `Any`
result path drags in the whole array-display stack. Asserting
`::JSON.Object{String,Any}` on the plain untyped parse is clean instead. The
varargs/iterator `Dict` constructors are likewise not trim-safe, hence the loop.

The parse is done in two stages. The strict parse is tried first because it is
the only one that preserves integers: `allownan=true` makes the parser produce
`Float64` for *every* number, so `{"y": 1}` would come back as `1.0` instead of
`1`, while the dynamic `getattrs` yields `Int64`. Only if the strict parse
throws - which happens for a document containing a bare `NaN`, `Infinity` or
`-Infinity` literal, invalid JSON that older Zarr.jl versions nevertheless
wrote, and that the dynamic method handles with its `": NaN," => ": \"NaN\","`
pre-pass - is the document retried with `allownan=true`. Integer attributes are
therefore widened to `Float64` only in such a bare-NaN/Infinity document.

A `Vector{UInt8}` (or `String`) can be parsed twice, so `maybecopy` is called
once and its result reused for the retry.
"""
function getattrs_typed(::ZarrFormat{2}, s::AbstractStore, p)
  atts = s[p, ".zattrs"]
  d = Dict{String,Any}()
  if atts === nothing
    return d
  end
  bytes = maybecopy(atts)
  local obj::JSON.Object{String,Any}
  try
    obj = JSON.parse(bytes)::JSON.Object{String,Any}
  catch
    obj = JSON.parse(bytes; allownan=true)::JSON.Object{String,Any}
  end
  for (k, v) in obj
    d[k] = v
  end
  return d
end

"""
    getattrs_typed(::ZarrFormat{3}, s, p) -> Dict{String,Any}

Statically typed counterpart of `getattrs(::ZarrFormat{3}, s, p)`. Reads the
`"attributes"` object out of `zarr.json`; an absent key (or an absent
`zarr.json`) yields an empty `Dict`.

Same two-stage parse as the v2 method: the strict parse is tried first because
it is the only one that preserves integers, and only a document containing a
bare `NaN`/`Infinity` literal (which the dynamic method handles with a
`replace` pre-pass) is retried with `allownan=true`. The `"attributes"` value is
asserted `::JSON.Object{String,Any}` before the copy loop for the same reason
the whole document is: the iterator/varargs `Dict` constructors are not
trim-safe, hence the explicit `setindex!` loop.
"""
function getattrs_typed(::ZarrFormat{3}, s::AbstractStore, p)
  md = s[p, "zarr.json"]
  d = Dict{String,Any}()
  if md === nothing
    return d
  end
  bytes = maybecopy(md)
  local obj::JSON.Object{String,Any}
  try
    obj = JSON.parse(bytes)::JSON.Object{String,Any}
  catch
    obj = JSON.parse(bytes; allownan=true)::JSON.Object{String,Any}
  end
  if !haskey(obj, "attributes")
    return d
  end
  atts = obj["attributes"]::JSON.Object{String,Any}
  for (k, v) in atts
    d[k] = v
  end
  return d
end

function getattrs(::ZarrFormat{3}, s::AbstractStore, p)
  md = s[p, "zarr.json"]
  if md === nothing
    error("zarr.json not found")
  else
    md = JSON.parse(replace(String(maybecopy(md)), ": NaN," => ": \"NaN\","); dicttype=Dict{String,Any})
    return get(md, "attributes", Dict{String,Any}())
  end
end

"""
    NoAttrs()

Marker value meaning "this node has no user attributes". It is the default of
the `attrs` keyword of [`zcreate`](@ref) so that the dynamic, `Dict{String,Any}`
based JSON attribute writer is never instantiated for the (very common) case of
an array without attributes - `writeattrs` then emits the constant `{}`
document. Arrays always store a plain `Dict` in their `attrs` field, see
[`attrsdict`](@ref).
"""
struct NoAttrs <: AbstractDict{String,Any} end
Base.length(::NoAttrs) = 0
Base.isempty(::NoAttrs) = true
Base.iterate(::NoAttrs, state...) = nothing
Base.haskey(::NoAttrs, ::Any) = false
Base.get(::NoAttrs, ::Any, default) = default
Base.getindex(::NoAttrs, k) = throw(KeyError(k))

"""Convert an `attrs` keyword value into the `Dict` stored on a `ZArray`/`ZGroup`."""
attrsdict(d::AbstractDict) = d
attrsdict(::NoAttrs) = Dict{String,Any}()


_empty_json_object() = UInt8[0x7b, 0x7d]  # "{}"

writeattrs(::ZarrFormat{2}, s::AbstractStore, p, ::NoAttrs; indent_json::Bool=false) =
  (s[p,".zattrs"] = _empty_json_object(); NoAttrs())

writeattrs(v::ZarrFormat{3}, s::AbstractStore, p, ::NoAttrs; indent_json::Bool=false) =
  writeattrs(v, s, p, Dict{String,Any}(); indent_json=indent_json)

function writeattrs(::ZarrFormat{2}, s::AbstractStore, p, att::Dict; indent_json::Bool=false)
  # Fast path for the (very common) empty-attribute case: serialising an empty
  # `Dict` through JSON pulls in the whole dynamic serialiser for no benefit,
  # while the result is always the two-byte literal "{}".
  if isempty(att)
    s[p,".zattrs"] = _empty_json_object()
    return att
  end
  b = IOBuffer()

  if indent_json
    JSON.print(b,att,4)
  else
    JSON.print(b,att)
  end

  s[p,".zattrs"] = take!(b)
  att
end

function writeattrs(::ZarrFormat{3}, s::AbstractStore, p, att::Dict; indent_json::Bool=false)
  # This is messy, we need to open zarr.json and replace the attributes section
  md = s[p, "zarr.json"]
  if md === nothing
    error("zarr.json not found")
  else
    md = JSON.parse(replace(String(maybecopy(md)), ": NaN," => ": \"NaN\","))
  end
  md = Dict(md)
  md["attributes"] = att

  b = IOBuffer()

  if indent_json
    JSON.print(b, md, 4)
  else
    JSON.print(b, md)
  end

  s[p, "zarr.json"] = take!(b)
  att
end

is_zarr3(s::AbstractStore, p) = isinitialized(s,_concatpath(p,"zarr.json"))
is_zarr2(s::AbstractStore, p) = is_zarray(ZarrFormat(Val(2)), s, p) || is_zgroup(ZarrFormat((Val(2))), s, p)

is_zgroup(::ZarrFormat{2}, s::AbstractStore, p) = isinitialized(s, _concatpath(p, ".zgroup"))
is_zarray(::ZarrFormat{2}, s::AbstractStore, p) = isinitialized(s, _concatpath(p, ".zarray"))
function is_zgroup(::ZarrFormat{3}, s::AbstractStore, p)
  isinitialized(s, _concatpath(p, "zarr.json")) || return false
  try
    metadata = getmetadata(ZarrFormat(Val(3)), s, p, false)
    metadata.node_type == "group"
  catch e
    e isa ArgumentError || rethrow()
    @warn "Skipping $p: $(e.msg)"
    false
  end
end
function is_zarray(::ZarrFormat{3}, s::AbstractStore, p)
  isinitialized(s, _concatpath(p, "zarr.json")) || return false
  try
    metadata = getmetadata(ZarrFormat(Val(3)), s, p, false)
    metadata.node_type == "array"
  catch e
    e isa ArgumentError || rethrow()
    @warn "Skipping $p: $(e.msg)"
    false
  end
end


isinitialized(s::AbstractStore, p, i::AbstractString) = isinitialized(s, _concatpath(p, i))
isinitialized(s::AbstractStore, i::AbstractString) = s[i] !== nothing

getmetadata(::ZarrFormat{2}, s::AbstractStore, p, fill_as_missing) = Metadata(String(maybecopy(s[p, ".zarray"])), fill_as_missing)

"""
    getmetadata(::Type{T}, ::Val{N}, ::Type{C}, ::ZarrFormat{2}, s, p, fill_as_missing::Bool)

Statically typed counterpart of `getmetadata(::ZarrFormat{2}, s, p, fill_as_missing)`.
Reads `.zarray` at path `p` and validates it against the caller-supplied
element type `T`, dimensionality `N` and compressor type `C`, so that the
returned metadata (and hence the `ZArray` built from it) has fully concrete
type parameters. Used by `zopen(::Type{T}, ::Val{N}, ...)`.
"""
function getmetadata(::Type{T}, ::Val{N}, ::Type{C}, ::ZarrFormat{2}, s::AbstractStore, p,
                     fill_as_missing::Bool) where {T,N,C<:Compressor}
  bytes = s[p, ".zarray"]
  if bytes === nothing
    throw(ArgumentError("no Zarr v2 array at the given path: .zarray not found"))
  end
  return MetadataV2(T, Val(N), C, parse_zarray(maybecopy(bytes)), fill_as_missing)
end

getmetadata(::ZarrFormat{3}, s::AbstractStore, p, fill_as_missing) = Metadata(String(maybecopy(s[p, "zarr.json"])), fill_as_missing)

"""
    getmetadata(::Type{T}, ::Val{N}, ::Type{P}, ::ZarrFormat{3}, s, p, fill_as_missing::Bool)

Statically typed counterpart of `getmetadata(::ZarrFormat{3}, s, p, fill_as_missing)`.
Reads `zarr.json` at path `p` and validates it against the caller-supplied
element type `T`, dimensionality `N` and codec pipeline type `P`, so that the
returned metadata (and hence the `ZArray` built from it) has fully concrete
type parameters. Used by `zopen(::Type{T}, ::Val{N}, ...; pipeline = P)`.
"""
function getmetadata(::Type{T}, ::Val{N}, ::Type{P}, ::ZarrFormat{3}, s::AbstractStore, p,
                     fill_as_missing::Bool) where {T,N,P<:V3Pipeline}
  bytes = s[p, "zarr.json"]
  if bytes === nothing
    throw(ArgumentError("no Zarr v3 array at the given path: zarr.json not found"))
  end
  return MetadataV3(T, Val(N), P, parse_zarrjson(maybecopy(bytes)), fill_as_missing)
end

function writemetadata(::ZarrFormat{2}, s::AbstractStore, p, m::AbstractMetadata; indent_json::Bool=false)
  met = IOBuffer()

  print_metadata(met, m, indent_json)

  s[p,".zarray"] = take!(met)
  m
end
function writemetadata(::ZarrFormat{3}, s::AbstractStore, p, m::AbstractMetadata; indent_json::Bool=false)
  met = IOBuffer()

  print_metadata(met, m, indent_json)

  s[p, "zarr.json"] = take!(met)
  m
end

"""
    write_new_metadata(v::ZarrFormat, s, p, m, attrs; indent_json=false)

Write the metadata *and* the attributes of a freshly created node. Used by
[`zcreate`](@ref); the split `writemetadata` + `writeattrs` pair remains for
updates to an existing node.

For v2 this is exactly the old pair (`.zarray` then `.zattrs`). For v3 both live
in the single `zarr.json` document, and writing them separately means
`writeattrs(::ZarrFormat{3}, ...)` has to read the document back and re-emit it
through the dynamic `Dict{String,Any}` JSON writer - which is not
`juliac --trim=safe` clean. Here the `attributes` object is instead part of the
initial NamedTuple.

Two v3 methods keep the empty-attribute case (`NoAttrs()`, the `zcreate`
default) off the dynamic serialiser entirely: it emits the constant `{}`
fragment. The `AbstractDict` method takes the same shortcut when the dict is
empty, but is only compiled at all if a dict is actually passed.
"""
function write_new_metadata(v::ZarrFormat{2}, s::AbstractStore, p, m::AbstractMetadata, attrs;
                            indent_json::Bool=false)
  writemetadata(v, s, p, m; indent_json=indent_json)
  writeattrs(v, s, p, attrs; indent_json=indent_json)
  m
end

function write_new_metadata(::ZarrFormat{3}, s::AbstractStore, p, m::MetadataV3, ::NoAttrs;
                            indent_json::Bool=false)
  met = IOBuffer()
  _print_metadata_v3(met, m, fill_value_encoding(m.fill_value), _EMPTY_JSON_OBJECT, indent_json)
  s[p, "zarr.json"] = take!(met)
  m
end

function write_new_metadata(::ZarrFormat{3}, s::AbstractStore, p, m::MetadataV3, attrs::AbstractDict;
                            indent_json::Bool=false)
  met = IOBuffer()
  fv = fill_value_encoding(m.fill_value)
  if isempty(attrs)
    _print_metadata_v3(met, m, fv, _EMPTY_JSON_OBJECT, indent_json)
  else
    _print_metadata_v3(met, m, fv, attrs, indent_json)
  end
  s[p, "zarr.json"] = take!(met)
  m
end

# Fallback for any other v3 metadata (e.g. group metadata): keep the old
# two-step behaviour.
function write_new_metadata(v::ZarrFormat{3}, s::AbstractStore, p, m::AbstractMetadata, attrs;
                            indent_json::Bool=false)
  writemetadata(v, s, p, m; indent_json=indent_json)
  writeattrs(v, s, p, attrs; indent_json=indent_json)
  m
end



## Handling sequential vs parallel IO
struct SequentialRead end
struct ConcurrentRead
    ntasks::Int
end
store_read_strategy(::AbstractStore) = SequentialRead()

"""
    has_configurable_missing_chunks(s::AbstractStore)

Trait indicating whether a store supports `missing_chunk_return_code!` for configuring
which HTTP error codes should be treated as "chunk not found" rather than errors.

Returns `false` by default. HTTP-based stores that support this should return `true`.
"""
has_configurable_missing_chunks(::AbstractStore) = false

"""
    missing_chunk_return_code!(s, code)

Add HTTP status codes that `s` should treat as missing keys.
"""
function missing_chunk_return_code! end

channelsize(s) = channelsize(store_read_strategy(s))
channelsize(::SequentialRead) = 0
channelsize(c::ConcurrentRead) = c.ntasks

read_items!(s::AbstractStore, c::AbstractChannel, e::AbstractChunkKeyEncoding, p, i) = read_items!(s, c, store_read_strategy(s), e, p, i)
function read_items!(s::AbstractStore, c::AbstractChannel, ::SequentialRead, e::AbstractChunkKeyEncoding, p, i)
    for ii in i
    res = store_readchunk(s, p, ii, e)
        put!(c,(ii=>res))
    end
end
function read_items!(s::AbstractStore, c::AbstractChannel, r::ConcurrentRead, e::AbstractChunkKeyEncoding, p, i)
    ntasks = r.ntasks
    #@show ntasks
    asyncmap(i,ntasks = ntasks) do ii
        #@show ii,objectid(current_task),p
    res = store_readchunk(s, p, ii, e)
        #@show ii,length(res)
        put!(c,(ii=>res))
        nothing
    end
end

"""
    store_putchunk(s, p, i, e, data)

Store the encoded chunk `data` under chunk index `i` of the array at path `p`,
or delete an already existing chunk when `data === nothing` (i.e. the chunk
consists entirely of the fill value).
"""
function store_putchunk(s::AbstractStore, p, i, e::AbstractChunkKeyEncoding, data)
  if data === nothing
    if store_isinitialized(s, p, i, e)
      store_deletechunk(s, p, i, e)
    end
  else
    store_writechunk(s, data, p, i, e)
  end
  nothing
end

write_items!(s::AbstractStore, c::AbstractChannel, e::AbstractChunkKeyEncoding, p, i) = write_items!(s, c, store_read_strategy(s), e, p, i)
function write_items!(s::AbstractStore, c::AbstractChannel, ::SequentialRead, e::AbstractChunkKeyEncoding, p, i)
  for _ in 1:length(i)
      ii,data = take!(c)
      store_putchunk(s, p, ii, e, data)
  end
  close(c)
end

function write_items!(s::AbstractStore, c::AbstractChannel, r::ConcurrentRead, e::AbstractChunkKeyEncoding, p, i)
  ntasks = r.ntasks
  asyncmap(i,ntasks = ntasks) do _
      ii,data = take!(c)
      store_putchunk(s, p, ii, e, data)
      nothing
  end
  close(c)
end

isemptysub(s::AbstractStore, p) = isempty(subkeys(s,p)) && isempty(subdirs(s,p))

"""
    StoreRegexList <: AbstractVector{Pair}

URL-pattern registry used by [`storefromstring`](@ref). The first match wins.
Entries are sorted by decreasing pattern length; equal-length entries retain
insertion order.
"""
struct StoreRegexList <: AbstractVector{Pair}
  entries::Vector{Pair}
end
StoreRegexList() = StoreRegexList(Pair[])

Base.size(l::StoreRegexList) = size(l.entries)
Base.getindex(l::StoreRegexList, i::Int) = l.entries[i]
Base.IndexStyle(::Type{StoreRegexList}) = IndexLinear()

# Longer regex patterns take precedence.
_regex_specificity(r::Regex) = ncodeunits(r.pattern)
_regex_specificity(x) = ncodeunits(string(x))
_entry_specificity(p::Pair) = _regex_specificity(first(p))

function _insert_by_specificity!(l::StoreRegexList, p::Pair, ties_first::Bool)
  s = _entry_specificity(p)
  i = if ties_first
    findfirst(e -> _entry_specificity(e) <= s, l.entries)
  else
    findfirst(e -> _entry_specificity(e) < s, l.entries)
  end
  i === nothing ? push!(l.entries, p) : insert!(l.entries, i, p)
  return l
end

Base.push!(l::StoreRegexList, p::Pair) = _insert_by_specificity!(l, p, false)
# Preserve pushfirst! precedence for equal-length patterns.
Base.pushfirst!(l::StoreRegexList, p::Pair) = _insert_by_specificity!(l, p, true)

const storageregexlist = StoreRegexList()

#include("formattedstore.jl")
include("directorystore.jl")
include("dictstore.jl")
include("consolidated.jl")
include("cachingstore.jl")
