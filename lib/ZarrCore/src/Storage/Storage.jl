
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

List the objects and common prefixes stored under path `p` in an object-store
backend, in whatever shape that backend's list API returns.

This is the shared building block for [`subdirs`](@ref), [`subkeys`](@ref) and
[`storagesize`](@ref) on cloud stores. The generic is declared here rather than
next to any one implementation because the implementations live in different
packages -- `GCStore` in ZarrGCS, `S3Store` in ZarrS3's `ZarrS3AWSS3Ext`
extension -- and both have to add methods to the *same* function.
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

function getattrs(::ZarrFormat{3}, s::AbstractStore, p)
  md = s[p, "zarr.json"]
  if md === nothing
    error("zarr.json not found")
  else
    md = JSON.parse(replace(String(maybecopy(md)), ": NaN," => ": \"NaN\","); dicttype=Dict{String,Any})
    return get(md, "attributes", Dict{String,Any}())
  end
end

function writeattrs(::ZarrFormat{2}, s::AbstractStore, p, att::Dict; indent_json::Bool=false)
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

getmetadata(::ZarrFormat{3}, s::AbstractStore, p, fill_as_missing) = Metadata(String(maybecopy(s[p, "zarr.json"])), fill_as_missing)

function writemetadata(::ZarrFormat{2}, s::AbstractStore, p, m::AbstractMetadata; indent_json::Bool=false)
  met = IOBuffer()

  if indent_json
    JSON.print(met,m,4)
  else
    JSON.print(met,m)
  end
  
  s[p,".zarray"] = take!(met)
  m
end
function writemetadata(::ZarrFormat{3}, s::AbstractStore, p, m::AbstractMetadata; indent_json::Bool=false)
  met = IOBuffer()

  if indent_json
    JSON.print(met, m, 4)
  else
    JSON.print(met, m)
  end

  s[p, "zarr.json"] = take!(met)
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
    missing_chunk_return_code!(s::AbstractStore, code::Union{Integer,AbstractVector{<:Integer}})

Extend the list of HTTP return codes that signal that a certain key in `s` is not
available. Most data providers return code 404 for missing elements, but some use
different return codes like 403. Use this to add the codes that signal a missing
chunk for a particular server; see [`has_configurable_missing_chunks`](@ref) for
whether a given store supports it.

### Example

````julia
a = zopen("https://path/to/remote/array")
missing_chunk_return_code!(a.storage, 403)
````

The generic is declared here, without any method, because its methods are spread
across packages: `HTTPStore` implements it in ZarrHTTP while the wrapper stores
that forward to it (`CachingStore`, `ConsolidatedStore`) stay in ZarrCore. Both
sides have to add methods to the *same* function.
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

write_items!(s::AbstractStore, c::AbstractChannel, e::AbstractChunkKeyEncoding, p, i) = write_items!(s, c, store_read_strategy(s), e, p, i)
function write_items!(s::AbstractStore, c::AbstractChannel, ::SequentialRead, e::AbstractChunkKeyEncoding, p, i)
  for _ in 1:length(i)
      ii,data = take!(c)
      if data === nothing
        if store_isinitialized(s, p, ii, e)
        store_deletechunk(s, p, ii, e)
        end
      else
      store_writechunk(s, data, p, ii, e)
      end
  end
  close(c)
end

function write_items!(s::AbstractStore, c::AbstractChannel, r::ConcurrentRead, e::AbstractChunkKeyEncoding, p, i)
  ntasks = r.ntasks
  asyncmap(i,ntasks = ntasks) do _
      ii,data = take!(c)
      if data === nothing
        if store_isinitialized(s, p, ii, e)
        store_deletechunk(s, p, ii, e)
        end
      else
      store_writechunk(s, data, p, ii, e)
      end
      nothing
  end
  close(c)
end

isemptysub(s::AbstractStore, p) = isempty(subkeys(s,p)) && isempty(subdirs(s,p))

#Here different storage backends can register regexes that are checked against
#during auto-check of storage format when doing zopen

"""
    StoreRegexList <: AbstractVector{Pair}

The registry backing [`storageregexlist`](@ref): a list of `Regex => storetype`
pairs that [`storefromstring`](@ref) uses to guess a store type from a URL-like
string.

`storefromstring` walks the list and takes the **first** entry whose regex
matches, so the order of the list decides the winner whenever several patterns
match the same string -- e.g. both `r"^https://storage.googleapis.com"` and
`r"^https://"` match a GCS URL, and only the former gives the right store.

To keep that decision independent of the order in which backends happen to
register themselves (which is not controllable once the backends live in
separate packages that may be loaded in any order), entries are kept sorted
most-specific-first instead of in insertion order. Specificity is approximated
by the length of the regex pattern, on the grounds that a pattern which refines
another one by spelling out more of the URL is the longer of the two. Entries of
equal specificity keep their relative registration order.

Backends register in the usual way and do not need to care about placement:

```julia
push!(storageregexlist, r"^myproto://" => MyStore)
```
"""
struct StoreRegexList <: AbstractVector{Pair}
  entries::Vector{Pair}
end
StoreRegexList() = StoreRegexList(Pair[])

Base.size(l::StoreRegexList) = size(l.entries)
Base.getindex(l::StoreRegexList, i::Int) = l.entries[i]
Base.IndexStyle(::Type{StoreRegexList}) = IndexLinear()

# How specific is a registered pattern? A longer pattern matches a subset of
# what the shorter pattern it extends matches, which is all that is needed to
# rank `r"^https://storage.googleapis.com"` above `r"^https://"`.
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
# `pushfirst!` used to be how a backend said "my pattern is more specific than
# something already in the list". The ordering above does that now, so this is
# only kept so existing registrations keep working; it differs from `push!`
# solely in that it wins ties against equally specific entries.
Base.pushfirst!(l::StoreRegexList, p::Pair) = _insert_by_specificity!(l, p, true)

const storageregexlist = StoreRegexList()
# Deliberately empty: every URL-addressable backend now lives in a subpackage
# (`ZarrS3`, `ZarrGCS`, `ZarrHTTP`) and registers itself from its `__init__`.

#include("formattedstore.jl")
include("directorystore.jl")
include("dictstore.jl")
include("consolidated.jl")
include("cachingstore.jl")
