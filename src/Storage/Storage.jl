
# Defines different storages for zarr arrays. Currently only regular files (DirectoryStore)
# and Dictionaries are supported
abstract type AbstractStore end

citostring(i::CartesianIndex) = join(reverse((i - one(i)).I), '.')

Base.getindex(s::AbstractStore, i::CartesianIndex) = s[citostring(i)]

function getattrs(s::AbstractStore)
  atts = s[".zattrs"]
  if atts === nothing
    Dict()
  else
    JSON.parse(replace(String(copy(atts)),": NaN,"=>": \"NaN\","))
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
