using AWSCore
using AWSSDK.S3

struct S3Store <: AbstractStore
    bucket::String
    store::String
    region::String
    aws::Dict{Symbol, Any}
    S3Store(b, s, r, a) = new(b, s, r, a)
end

S3Store(bucket::String, store::String, region::String) = S3Store(bucket, store, region, aws_config(creds=nothing,region=region))

S3Store(s::S3Store, d::String) = S3Store(s.bucket, d, s.region, s.aws)

Base.show(io::IO,s::S3Store) = print(io,"S3 Object Storage")

function Base.getindex(s::S3Store, i::String)
  try
    return S3.get_object(s.aws,Bucket=s.bucket,Key=joinpath(s.store,i))
  catch
    return nothing
  end
end

function storagesize(s::S3Store)
    contents = S3.list_objects_v2(s.aws, Bucket=s.bucket, prefix=s.store)["Contents"]
    sum(filter(entry -> !any(filename -> endswith(entry["Key"], filename), [".zattrs",".zarray",".zgroup"]), contents)) do f
        parse(Int, f["Size"])
    end
end

zname(s::S3Store) = splitdir(splitdir(s.store)[1])[2]

function isinitialized(s::S3Store, i::String)
  try
    S3.head_object(s.aws,Bucket=s.bucket,Key=joinpath(s.store,i))
    return true
  catch
    return false
  end
end

is_zgroup(s::S3Store) = isinitialized(s,".zgroup")
is_zarray(s::S3Store) = isinitialized(s,".zarray")

function subs(s::S3Store)
  st = endswith(s.store,"/") ? s.store : string(s.store,"/")
  s3_resp = S3.list_objects_v2(s.aws, Bucket=s.bucket, prefix=st, delimiter = "/")
  allstrings(s3_resp["CommonPrefixes"])
end
allstrings(v::AbstractArray) = map(i -> String(i["Prefix"]), v)
allstrings(v) = [String(v["Prefix"])]

path(s::S3Store) = s.store
