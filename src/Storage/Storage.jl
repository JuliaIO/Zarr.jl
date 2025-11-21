
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


"""
    Base.delete!(d::AbstractStore, k::String)

Deletes the given key from the store.
"""

citostring(i::CartesianIndex) = join(reverse((i - oneunit(i)).I), '.')
citostring(::CartesianIndex{0}) = "0"
_concatpath(p,s) = isempty(p) ? s : rstrip(p,'/') * '/' * s

Base.getindex(s::AbstractStore, p, i::CartesianIndex) = s[p, citostring(i)]
Base.getindex(s::AbstractStore, p, i) = s[_concatpath(p,i)]
Base.delete!(s::AbstractStore, p, i::CartesianIndex) = delete!(s, p, citostring(i))
Base.delete!(s::AbstractStore, p, i) = delete!(s, _concatpath(p,i))
Base.haskey(s::AbstractStore, k) = isinitialized(s,k)
Base.setindex!(s::AbstractStore,v,p,i) = setindex!(s,v,_concatpath(p,i))
Base.setindex!(s::AbstractStore,v,p,i::CartesianIndex) = s[p, citostring(i)]=v


maybecopy(x) = copy(x)
maybecopy(x::String) = x


function getattrs(s::AbstractStore, p)
  atts = s[p,".zattrs"]
  if atts === nothing
    Dict()
  else
    JSON.parse(replace(String(maybecopy(atts)),": NaN,"=>": \"NaN\","); dicttype = Dict{String,Any})
  end
end
function writeattrs(s::AbstractStore, p, att::Dict; indent_json::Bool= false)
  b = IOBuffer()

  if indent_json
    JSON.print(b,att,4)
  else
    JSON.print(b,att)
  end

  s[p,".zattrs"] = take!(b)
  att
end

is_zgroup(s::AbstractStore, p) = isinitialized(s,_concatpath(p,".zgroup"))
is_zarray(s::AbstractStore, p) = isinitialized(s,_concatpath(p,".zarray"))

isinitialized(s::AbstractStore, p, i::CartesianIndex)=isinitialized(s,p,citostring(i))
isinitialized(s::AbstractStore, p, i) = isinitialized(s,_concatpath(p,i))
isinitialized(s::AbstractStore, i) = s[i] !== nothing

getmetadata(s::AbstractStore, p,fill_as_missing) = Metadata(String(maybecopy(s[p,".zarray"])),fill_as_missing)
function writemetadata(s::AbstractStore, p, m::Metadata; indent_json::Bool= false)
  met = IOBuffer()

  if indent_json
    JSON.print(met,m,4)
  else
    JSON.print(met,m)
  end
  
  s[p,".zarray"] = take!(met)
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

read_items!(s::AbstractStore,c::AbstractChannel, p, i) = read_items!(s,c,store_read_strategy(s),p,i)
function read_items!(s::AbstractStore,c::AbstractChannel, ::SequentialRead ,p,i)
    for ii in i
        res = s[p,ii]
        put!(c,(ii=>res))
    end
end
function read_items!(s::AbstractStore,c::AbstractChannel, r::ConcurrentRead ,p,i)
    ntasks = r.ntasks
    #@show ntasks
    asyncmap(i,ntasks = ntasks) do ii
        #@show ii,objectid(current_task),p
        res = s[p,ii]
        #@show ii,length(res)
        put!(c,(ii=>res))
        nothing
    end
end

write_items!(s::AbstractStore,c::AbstractChannel, p, i) = write_items!(s,c,store_read_strategy(s),p,i)
function write_items!(s::AbstractStore,c::AbstractChannel, ::SequentialRead ,p,i)
  for _ in 1:length(i)
      ii,data = take!(c)
      if data === nothing
        if isinitialized(s,p,ii)
          delete!(s,p,ii)
        end
      else
        s[p,ii] = data
      end
  end
  close(c)
end

function write_items!(s::AbstractStore,c::AbstractChannel, r::ConcurrentRead ,p,i)
  ntasks = r.ntasks
  asyncmap(i,ntasks = ntasks) do _
      ii,data = take!(c)
      if data === nothing
        if isinitialized(s,ii)
          delete!(s,ii)
        end
      else
        s[p,ii] = data
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

include("directorystore.jl")
include("dictstore.jl")
include("gcstore.jl")
include("consolidated.jl")
include("http.jl")
include("zipstore.jl")
