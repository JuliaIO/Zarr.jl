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
  catch
    return nothing
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
  d = splitdir(s.store)
  i = findlast(!isempty,d)
  d[i]
end

function isinitialized(s::S3Store, i::String)
  try
    S3.head_object(s.aws,Bucket=s.bucket,Key=joinpath(s.store,i))
    return true
  catch
    return false
  end
end

function subdirs(s::S3Store)
  st = endswith(s.store,"/") ? s.store : string(s.store,"/")
  s3_resp = S3.list_objects_v2(s.aws, Bucket=s.bucket, prefix=st, delimiter = "/")
  allstrings(s3_resp["CommonPrefixes"])
end
function Base.keys(s::S3Store)
  st = endswith(s.store,"/") ? s.store : string(s.store,"/")
  s3_resp = S3.list_objects_v2(s.aws, Bucket=s.bucket, prefix=st, delimiter = "/")
  allstrings(s3_resp["Contents"])
end
allstrings(v::AbstractArray) = map(i -> String(i["Prefix"]), v)
allstrings(v) = [String(v["Prefix"])]

path(s::S3Store) = s.store
