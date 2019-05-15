using AWSCore
using AWSSDK.S3

struct S3Store <: AbstractStore
    bucket::String
    store::String
    region::String
    aws::Dict{Symbol, Any}
end

S3Store(bucket::String, store::String, region::String) = S3Store(bucket, store, region, aws_config(creds=nothing,region=region))

Base.show(io::IO,s::S3Store) = print(io,"S3 Object Storage")

function Base.getindex(s::S3Store, i::String)
  try
    return S3.get_object(s.aws,Bucket=s.bucket,Key=joinpath(s.store,i))
  catch e
    if isa(e,AWSCore.AWSException) && (e.code=="NoSuchKey" || e.code=="404")
      return nothing
    else
      throw(e)
    end
  end
end
getsub(s::S3Store, d::String) = S3Store(s.bucket, joinpath(s.store,d), s.region, s.aws)

function storagesize(s::S3Store)
    contents = S3.list_objects_v2(s.aws, Bucket=s.bucket, prefix=s.store)["Contents"]
    sum(filter(entry -> !any(filename -> endswith(entry["Key"], filename), [".zattrs",".zarray",".zgroup"]), contents)) do f
        parse(Int, f["Size"])
    end
end

function zname(s::S3Store)
  d = split(s.store,"/")
  i = findlast(!isempty,d)
  d[i]
end

function isinitialized(s::S3Store, i::String)
  try
    S3.head_object(s.aws,Bucket=s.bucket,Key=joinpath(s.store,i))
    return true
  catch e

    if isa(e,AWSCore.AWSException) && (e.code=="NoSuchKey" || e.code=="404")
      return false
    else
      throw(e)
    end
  end
end

isgoogle(s::S3Store) = get(s.aws,:url_ext,nothing) == "googleapis.com"

function cloud_list_objects(s::S3Store, prefix)
  if isgoogle(s)
    S3.list_objects(s.aws, Bucket=s.bucket, prefix=prefix, delimiter = "/")
  else
    S3.list_objects_v2(s.aws, Bucket=s.bucket, prefix=prefix, delimiter = "/")
  end
end
function subdirs(s::S3Store)
  st = (isempty(s.store) || endswith(s.store,"/")) ? s.store : string(s.store,"/")
  s3_resp = cloud_list_objects(s,st)
  !haskey(s3_resp,"CommonPrefixes") && return String[]
  allstrings(s3_resp["CommonPrefixes"],"Prefix")
end
function Base.keys(s::S3Store)
  st = endswith(s.store,"/") ? s.store : string(s.store,"/")
  s3_resp = cloud_list_objects(s,st)
  !haskey(s3_resp,"Contents") && return String[]
  r = allstrings(s3_resp["Contents"],"Key")
  map(i->splitdir(i)[2],r)
end
allstrings(v::AbstractArray,prefixkey) = map(i -> String(i[prefixkey]), v)
allstrings(v,prefixkey) = [String(v[prefixkey])]

path(s::S3Store) = s.store
