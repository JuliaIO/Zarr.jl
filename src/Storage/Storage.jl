
# Defines different storages for zarr arrays. Currently only regular files (DirectoryStore)
# and Dictionaries are supported

# Default Zarr version
const DV = 2

# Default Zarr separator

# Default Zarr v2 separator
const DS2 = '.'
# Default Zarr v3 separator
const DS3 = '/'

default_sep(version) = version == 2 ? DS2 : DS3
const DS = default_sep(DV)

"""
    abstract type AbstractStore{V,S}

This the abstract supertype for all Zarr store implementations.  Currently only regular files ([`DirectoryStore`](@ref))
and Dictionaries are supported.

# Type Parameters
V is the version, either 2 or 3
S is the dimension separator

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
abstract type AbstractStore{V,S} end

#Define the interface
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

@inline citostring(i::CartesianIndex, version::Int=DV, sep::Char=default_sep(version)) = (version == 3 ? "c$sep" : "" ) * join(reverse((i - oneunit(i)).I), sep)
@inline citostring(::CartesianIndex{0}, version::Int=DV, sep::Char=default_sep(version)) = (version == 3 ? "c$(sep)0" : "0" )
citostring(i::CartesianIndex, s::AbstractStore{V, S}) where {V,S} = citostring(i, V, S)
_concatpath(p,s) = isempty(p) ? s : rstrip(p,'/') * '/' * s

Base.getindex(s::AbstractStore, p, i::CartesianIndex) = s[p, citostring(i, s)]
Base.getindex(s::AbstractStore, p, i) = s[_concatpath(p,i)]
Base.delete!(s::AbstractStore, p, i::CartesianIndex) = delete!(s, p, citostring(i, s))
Base.delete!(s::AbstractStore, p, i) = delete!(s, _concatpath(p,i))
Base.haskey(s::AbstractStore, k) = isinitialized(s,k)
Base.setindex!(s::AbstractStore,v,p,i) = setindex!(s,v,_concatpath(p,i))
Base.setindex!(s::AbstractStore,v,p,i::CartesianIndex) = s[p, citostring(i, s)]=v


maybecopy(x) = copy(x)
maybecopy(x::String) = x


function getattrs(s::AbstractStore, p)
  atts = s[p,".zattrs"]
  if atts === nothing
    Dict()
  else
    JSON.parse(replace(String(maybecopy(atts)),": NaN,"=>": \"NaN\","))
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

isinitialized(s::AbstractStore, p, i::CartesianIndex) = isinitialized(s,p,citostring(i, s))
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

function Base.getproperty(store::AbstractStore{V,S}, sym::Symbol) where {V,S}
    if sym == :dimension_separator
        return S
    elseif sym == :zarr_format
        return V
    else
        return getfield(store, sym)
    end
end
function Base.propertynames(store::AbstractStore)
    return (:dimension_separator, :version, getfields(store)...)
end

include("directorystore.jl")
include("dictstore.jl")
include("s3store.jl")
include("gcstore.jl")
include("consolidated.jl")
include("http.jl")
include("zipstore.jl")

# Itemize subtypes of AbstractStore for code generation below
const KnownAbstractStores = (DirectoryStore, GCStore, S3Store, ConsolidatedStore, DictStore, HTTPStore, ZipStore)

"""
    Zarr.set_dimension_separator(::AbstractStore{V}, sep::Char)::AbstractStore{V,sep}

Returns an AbstractStore of the same type with the same `zarr_format` parameter, `V`,
but with a dimension separator of `sep`.

# Examples

```
julia> Zarr.set_dimension_separator(Zarr.DictStore{2, '.'}(), '/') |> typeof
Zarr.DictStore{2, '/'}
```
 
"""
set_dimension_separator

"""
    set_zarr_format(::AbstractStore{<: Any, S}, zarr_format::Int)::AbstractStore{zarr_format,S}

Returns an AbstractStore of the same type with the same `dimension_separator` parameter, `S`,
but with the specified `zarr_format` parameter.

# Examples

```
julia> Zarr.set_zarr_format(Zarr.DictStore{2, '.'}(), 3) |> typeof
Zarr.DictStore{3, '.'}
```

"""
set_zarr_format

for T in KnownAbstractStores
    e = quote
        # copy constructor to change zarr_format and dimension_separator parameters
        (::Type{$T{V,S}})(store::$T) where {V,S} =
            $T{V,S}(ntuple(i->getfield(store, i), nfields(store))...)
        set_dimension_separator(store::$T{V}, sep::Char) where V =
            $T{V,sep}(ntuple(i->getfield(store, i), nfields(store))...)
        set_zarr_format(store::$T{<: Any, S}, zarr_format::Int) where S =
            $T{zarr_format,S}(ntuple(i->getfield(store, i), nfields(store))...)
    end
    eval(e)
end
