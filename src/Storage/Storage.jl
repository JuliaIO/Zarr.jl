
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
  S3Store(bucket::String; aws=nothing)
 
An S3-backed Zarr store. Available after loading the `ZarrAWSS3Ext` extension.
"""
struct S3Store <: AbstractStore
    bucket::String
    aws::Any
end

function S3Store(args...)
    error("AWSS3 must be loaded to use S3Store. Try `using AWSS3`.")
end

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

# Default Zarr v2 separator
const DS2 = '.'
# Default Zarr v3 separator
const DS3 = '/'

default_sep(::ZarrFormat{2}) = DS2
default_sep(::ZarrFormat{3}) = DS3
default_prefix(::ZarrFormat{2}) = false
default_prefix(::ZarrFormat{3}) = true
const DS = default_sep(DV)

ZarrFormat(s::AbstractStore, path) = is_zarr2(s, path) ? ZarrFormat(2) :
                                     is_zarr3(s, path) ? ZarrFormat(3) :
                                     throw(ArgumentError("Specified store $s in path $(path) is neither a ZArray nor a ZGroup in a recognized zarr format."))


@inline function citostring(e::ChunkEncoding, i::CartesianIndex)
  if e.prefix
    "c$(e.sep)" * join(reverse((i - oneunit(i)).I), e.sep)
  else
    join(reverse((i - oneunit(i)).I), e.sep)
  end
end
@inline citostring(e::ChunkEncoding, ::CartesianIndex{0}) = e.prefix ? "c$(e.sep)0" : "0"

_concatpath(p,s) = isempty(p) ? s : rstrip(p,'/') * '/' * s

# Function to read a chunk from store s
store_readchunk(s::AbstractStore, p, i::CartesianIndex, e::ChunkEncoding) = s[p, citostring(e, i)]
store_deletechunk(s::AbstractStore, p, i::CartesianIndex, e::ChunkEncoding) = delete!(s, p, citostring(e, i))
store_writechunk(s::AbstractStore, v, p, i::CartesianIndex, e::ChunkEncoding) = s[p, citostring(e, i)] = v
store_isinitialized(s::AbstractStore, p, i::CartesianIndex, e::ChunkEncoding) = isinitialized(s, p, citostring(e, i))


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
    md = JSON.parse(replace(String(maybecopy(md)), ": NaN," => ": \"NaN\","))
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
is_zgroup(::ZarrFormat{3}, s::AbstractStore, p, metadata=getmetadata(s, p, false)) =
  isinitialized(s, _concatpath(p, "zarr.json")) &&
  metadata.node_type == "group"
is_zarray(::ZarrFormat{3}, s::AbstractStore, p, metadata=getmetadata(s, p, false)) =
  isinitialized(s, _concatpath(p, "zarr.json")) &&
  metadata.node_type == "array"


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

channelsize(s) = channelsize(store_read_strategy(s))
channelsize(::SequentialRead) = 0
channelsize(c::ConcurrentRead) = c.ntasks

read_items!(s::AbstractStore, c::AbstractChannel, e::ChunkEncoding, p, i) = read_items!(s, c, store_read_strategy(s), e, p, i)
function read_items!(s::AbstractStore, c::AbstractChannel, ::SequentialRead, e::ChunkEncoding, p, i)
    for ii in i
    res = store_readchunk(s, p, ii, e)
        put!(c,(ii=>res))
    end
end
function read_items!(s::AbstractStore, c::AbstractChannel, r::ConcurrentRead, e::ChunkEncoding, p, i)
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

write_items!(s::AbstractStore, c::AbstractChannel, e::ChunkEncoding, p, i) = write_items!(s, c, store_read_strategy(s), e, p, i)
function write_items!(s::AbstractStore, c::AbstractChannel, ::SequentialRead, e::ChunkEncoding, p, i)
  for _ in 1:length(i)
      ii,data = take!(c)
      if data === nothing
        if isinitialized(s,p,ii)
        store_deletechunk(s, p, ii, e)
        end
      else
      store_writechunk(s, data, p, ii, e)
      end
  end
  close(c)
end

function write_items!(s::AbstractStore, c::AbstractChannel, r::ConcurrentRead, e::ChunkEncoding, p, i)
  ntasks = r.ntasks
  asyncmap(i,ntasks = ntasks) do _
      ii,data = take!(c)
      if data === nothing
        if isinitialized(s,ii)
        store_deletechunk(s, p, ii, e)
        end
      else
      store_writechunk(s, data, p, ii, e) = data
      end
      nothing
  end
  close(c)
end

isemptysub(s::AbstractStore, p) = isempty(subkeys(s,p)) && isempty(subdirs(s,p))

#Here different storage backends can register regexes that are checked against
#during auto-check of storage format when doing zopen
storageregexlist = Pair[]
push!(storageregexlist, r"^s3://" => S3Store)

#include("formattedstore.jl")
include("directorystore.jl")
include("dictstore.jl")
include("gcstore.jl")
include("consolidated.jl")
include("http.jl")
include("zipstore.jl")
