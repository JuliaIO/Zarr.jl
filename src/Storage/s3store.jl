using AWSCore
using AWSSDK.S3

struct S3Store <: AbstractStore
    bucket::String
    store::String
    listversion::Int
    aws::Dict{Symbol, Any}
end


function S3Store(bucket::String, store::String;
  listversion = 2,
  aws = nothing,
  region = get(ENV, "AWS_DEFAULT_REGION", "us-east-1"),
  creds = nothing,
  )
  if aws === nothing
    aws = aws_config(creds=creds,region=region)
  end
  S3Store(bucket, store, listversion, aws)
end

Base.show(io::IO,s::S3Store) = print(io,"S3 Object Storage")

function error_is_ignorable(e)
  isa(e,AWSCore.AWSException) && (e.code=="NoSuchKey" || e.code=="404")
end

function Base.getindex(s::S3Store, i::String)
  try
    return S3.get_object(s.aws,Bucket=s.bucket,Key=joinpath(s.store,i))
  catch e
    if error_is_ignorable(e)
      return nothing
    else
      throw(e)
    end
  end
end
getsub(s::S3Store, d::String) = S3Store(s.bucket, joinpath(s.store,d), s.listversion, s.aws)

function storagesize(s::S3Store)
  r = cloud_list_objects(s)
  haskey(r,"Contents") || return 0
  contents = r["Contents"]
  datafiles = filter(entry -> !any(filename -> endswith(entry["Key"], filename), [".zattrs",".zarray",".zgroup"]), contents)
  if isempty(datafiles)
    0
  else
    sum(datafiles) do f
      parse(Int, f["Size"])
    end
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
    if error_is_ignorable(e)
      return false
    else
      println(joinpath(s.store,i))
      throw(e)
    end
  end
end

function cloud_list_objects(s::S3Store)
  prefix = (isempty(s.store) || endswith(s.store,"/")) ? s.store : string(s.store,"/")
  listfun = s.listversion==2 ? S3.list_objects_v2 : S3.list_objects
  listfun(s.aws, Bucket=s.bucket, prefix=prefix, delimiter = "/")
end
function subdirs(s::S3Store)
  s3_resp = cloud_list_objects(s)
  !haskey(s3_resp,"CommonPrefixes") && return String[]
  allstrings(s3_resp["CommonPrefixes"],"Prefix")
end
function Base.keys(s::S3Store)
  s3_resp = cloud_list_objects(s)
  !haskey(s3_resp,"Contents") && return String[]
  r = allstrings(s3_resp["Contents"],"Key")
  map(i->splitdir(i)[2],r)
end
allstrings(v::AbstractArray,prefixkey) = map(i -> String(i[prefixkey]), v)
allstrings(v,prefixkey) = [String(v[prefixkey])]

path(s::S3Store) = s.store
