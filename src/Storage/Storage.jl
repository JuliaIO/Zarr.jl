
# Defines different storages for zarr arrays. Currently only regular files (DirectoryStore)
# and Dictionaries are supported
abstract type AbstractStore end

#Define the interface
"""
  storagesize(d::AbstractStore)

This function shall return the size of all data files in a store.
"""
function storagesize end

"""
    zname(d::AbstractStore)

Returns the name of the current variable.
"""
function zname end

"""
    Base.getindex(d::AbstractStore,i::String)

Returns the data stored in the given key as a Vector{UInt8}
"""
Base.getindex(d::AbstractStore,i::String) = error("getindex not implemented for store $(typeof(d))")

"""
    Base.setindex!(d::AbstractStore,v,i::String)

Writes the values in v to the given store and key.
"""
Base.setindex!(d::AbstractStore,v,i::String) = error("setindex not implemented for store $(typeof(d))")

"""
    subdirs(d::AbstractStore)

Returns a list of keys for children stores in the given store.
"""
function subdirs end

"""
    Base.keys(d::AbstractStore)

Returns the keys of files in the given store.
"""
Base.keys(d::AbstractStore) = error("keys function not implemented for store $(typeof(d))")

"""
    newsub(d::AbstractStore, name::String)

Create a new Store as a child of the given store `d` with given `name`. Returns the new created substore.
"""
function newsub end

"""
    getsub(d::AbstractStore, name::String)

Returns the child store of name `name`.
"""
function getsub end

citostring(i::CartesianIndex) = join(reverse((i - one(i)).I), '.')

Base.getindex(s::AbstractStore, i::CartesianIndex) = s[citostring(i)]

maybecopy(x) = copy(x)
maybecopy(x::String) = x

function getattrs(s::AbstractStore)
  atts = s[".zattrs"]
  if atts === nothing
    Dict()
  else
    JSON.parse(replace(String(maybecopy(atts)),": NaN,"=>": \"NaN\","))
  end
end
function writeattrs(s::AbstractStore, att::Dict)
  b = IOBuffer()
  JSON.print(b,att)
  s[".zattrs"] = take!(b)
  att
end

is_zgroup(s::AbstractStore) = isinitialized(s,".zgroup")
is_zarray(s::AbstractStore) = isinitialized(s,".zarray")

isinitialized(s::AbstractStore, i::CartesianIndex)=isinitialized(s,citostring(i))

isinitialized(s::AbstractStore, i) = s[i] !== nothing

getmetadata(s::AbstractStore) = Metadata(String(s[".zarray"]))
function writemetadata(s::AbstractStore, m::Metadata)
  met = IOBuffer()
  JSON.print(met,m)
  s[".zarray"] = take!(met)
  m
end

Base.setindex!(s::AbstractStore,v,i::CartesianIndex) = s[citostring(i)]=v

Base.isempty(s::AbstractStore) = isempty(keys(s)) && isempty(subdirs(s))

include("directorystore.jl")
include("dictstore.jl")
include("s3store.jl")
