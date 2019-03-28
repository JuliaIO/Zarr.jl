# Defines different storages for zarr arrays. Currently only regular files (DirectoryStore)
# and Dictionaries are supported
import JSON

abstract type AbstractStore end

"Normalize logical storage path"
function normalize_path(p::AbstractString)
    p = replace(p, '\\'=>'/')
    p = normpath(p)
    #strip(p, '/') This removes / at the beginning of the path, bad for absolute paths on Linux
    ilast = findlast(!isequal('/'),p)
    p[1:ilast]
end

# Stores files in a regular file system
struct DirectoryStore <: AbstractStore
    folder::String
    DirectoryStore(p) = new(normalize_path(p))
end

"""
Creates a new DirectoryStore from given metadata by creating a folder on disk and writing the
.zarray and .zattrs files.
"""
function DirectoryStore(path,name,metadata,attrs)
  if isempty(name)
    name = splitdir(path)[2]
  else
    path = joinpath(path, name)
  end
  if isdir(path)
    !isempty(readdir(path)) && throw(ArgumentError("Directory $path is not empty"))
  else
    mkpath(path)
  end
  open(joinpath(path, ".zarray"), "w") do f
    JSON.print(f, metadata)
  end
  open(joinpath(path, ".zattrs"), "w") do f
    JSON.print(f, attrs)
  end
  DirectoryStore(path)
end

storagesize(d::DirectoryStore) = sum(filter(i->i ∉ (".zattrs",".zarray"),readdir(d.folder))) do f
  filesize(joinpath(d.folder,f))
end

function getattrs(s::DirectoryStore)
    if isfile(joinpath(s.folder, ".zattrs"))
      #Workaround to catch NaNs in user attributes seen issue https://github.com/zarr-developers/zarr/issues/412
      alllines = open(readlines,joinpath(s.folder, ".zattrs"))
      JSON.parse(replace(join(alllines,"\n"),": NaN,"=>": \"NaN\","))
    else
        Dict()
    end
end

function getchunk(s::DirectoryStore, i::CartesianIndex)
  f = joinpath(s.folder, join(reverse((i - one(i)).I), '.'))
  isfile(f) ? f : nothing
end
function createchunk(s::DirectoryStore, i::CartesianIndex)
  f = joinpath(s.folder, join(reverse((i - one(i)).I), '.'))
  @assert !isfile(f)
  open(i->nothing,f,"w")
  f
end

isinitialized(s::DirectoryStore, i::CartesianIndex) = isfile(joinpath(s.folder, join(reverse((i - one(i)).I), '.')))

function adddir(s::DirectoryStore, i::String)
    f = joinpath(s.folder, i)
    mkpath(f)
end

zname(s::DirectoryStore) = splitdir(s.folder)[2]


# Stores data in a simple dict in memory
struct DictStore{T} <: AbstractStore
    name::String
    a::T
    attrs::Dict
end
function DictStore(path,name,metadata,attrs)
  nsubs = map((s, c) -> ceil(Int, s/c), metadata.shape, metadata.chunks)
  et = areltype(metadata.compressor, eltype(metadata))
  T=eltype(metadata)
  isempty(name) && (name="data")
  a = Array{et}(undef, nsubs...)
  for i in eachindex(a)
    a[i] = T[]
  end
  DictStore(name, a, attrs)
end
Base.show(io::IO,d::DictStore) = print(io,"Dictionary Storage")

storagesize(d::DictStore) = sum(sizeof,values(d.a))
zname(s::DictStore) = s.name

"Returns the chunk at index i if present"
function getchunk(s::DictStore,  i::CartesianIndex)
    s.a[i]
end

"Checks if a chunk is initialized"
isinitialized(s::DictStore, i::CartesianIndex) = true
