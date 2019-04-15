# Defines different storages for zarr arrays. Currently only regular files (DirectoryStore)
# and Dictionaries are supported
import JSON
using AWSS3
using AWSCore
using AWSSDK.S3

abstract type AbstractStore end

"Normalize logical storage path"
function normalize_path(p::AbstractString)
    # \ to / since normpath on linux won't handle it
    p = replace(p, '\\'=>'/')
    p = normpath(p)
    # \ to / again since normpath on windows creates \
    p = replace(p, '\\'=>'/')
    p == "/" ? p : rstrip(p, '/')
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

function DirectoryStore(s::DirectoryStore, d::String)
    DirectoryStore(joinpath(s.folder), d)
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

is_zgroup(s::DirectoryStore) = isfile(joinpath(s.folder, ".zgroup"))
is_zarray(s::DirectoryStore) = isfile(joinpath(s.folder, ".zarray"))
readmeta(s::DirectoryStore, name::String) = read(joinpath(s.folder, name), String)

subs(s::DirectoryStore) = filter(i -> isdir(joinpath(s.folder, i)), readdir(s.folder))

path(s::DirectoryStore) = s.folder


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

# change when DictStore ZGroups are implemented
is_zgroup(s::DictStore) = false
is_zarray(s::DictStore) = true

struct S3Store <: AbstractStore
    bucket::String
    store::String
    region::String
    aws::Dict{Symbol, Any}
    S3Store(b, s, r, a) = new(b, s, r, a)
end

S3Store(bucket::String, store::String, region::String) = S3Store(bucket, store, region, aws_config(region=region))

S3Store(s::S3Store, d::String) = S3Store(s.bucket, d, s.region, s.aws)

Base.show(io::IO,s::S3Store) = print(io,"S3 Object Storage")

function S3Store(bucket, region, store, metadata, attrs)
end

function storagesize(s::S3Store)
    contents = S3.list_objects_v2(s.aws, Bucket=s.bucket, prefix=s.store)["Contents"]
    sum(filter(entry -> !any(filename -> endswith(entry["Key"], filename), [".zattrs",".zarray",".zgroup"]), contents)) do f
        parse(Int, f["Size"])
    end
end

function getattrs(s::S3Store)
    if s3_exists(s.aws, s.bucket, joinpath(s.store, ".zattrs"))
      #Workaround to catch NaNs in user attributes seen issue https://github.com/zarr-developers/zarr/issues/412
      JSON.parse(replace(String(s3_get(s.aws, s.bucket, joinpath(s.store, ".zattrs"))),": NaN,"=>": \"NaN\","))
    else
        Dict()
    end
end

function getchunk(s::S3Store, i::CartesianIndex)
    f = joinpath(s.store, join(reverse((i - one(i)).I), '.'))
    s3_exists(s.aws, s.bucket, f) ? f : nothing
end

function createchunk(s::S3Store, i::CartesianIndex)

end

function readobject(o::String, s::S3Store)
    return s3_get(s.aws, s.bucket, o)
end

zname(s::S3Store) = splitdir(splitdir(s.store)[1])[2]

isinitialized(s::S3Store, i::CartesianIndex) = s3_exists(s.aws, s.bucket, joinpath(s.store, join(reverse((i - one(i)).I), '.')))

is_zgroup(s::S3Store) = s3_exists(s.aws, s.bucket, joinpath(s.store, ".zgroup"))
is_zarray(s::S3Store) = s3_exists(s.aws, s.bucket, joinpath(s.store, ".zarray"))
readmeta(s::S3Store, name::String) = String(s3_get(s.aws, s.bucket, joinpath(s.store, name)))

function subs(s::S3Store)
    s3_resp = S3.list_objects_v2(s.aws, Bucket=s.bucket, prefix=s.store, delimiter = "/")
    if typeof(s3_resp["CommonPrefixes"]) <: AbstractArray
        return map(i -> String(i["Prefix"]), s3_resp["CommonPrefixes"])
    else
        return [String(s3_resp["CommonPrefixes"]["Prefix"])]
    end
end

path(s::S3Store) = s.store
