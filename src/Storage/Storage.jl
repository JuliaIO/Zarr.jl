
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
    JSON.parse(replace(String(atts),": NaN,"=>": \"NaN\","))
  end
end

isinitialized(s::AbstractStore, i::CartesianIndex)=isinitialized(s,citostring(i))

getmetadata(s::AbstractStore) = Metadata(String(s[".zarray"]))

function Base.setindex!(s::AbstractStore,v,i::CartesianIndex)
  s[citostring(i)]=v
end

include("directorystore.jl")
include("dictstore.jl")
include("s3store.jl")
