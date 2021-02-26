using AWS

@service S3

struct S3Store <: AbstractStore
    bucket::String
    store::String
    listversion::Int
    aws::AWS.AbstractAWSConfig
end


function S3Store(bucket::String, store::String;
  listversion = 2,
  aws = nothing,
  region = get(ENV, "AWS_DEFAULT_REGION", "us-east-1"),
  creds = nothing,
  )
  if aws === nothing
    aws = AWS.AWSConfig(creds=creds,region=region)
  end
  store = rstrip(store,'/')
  S3Store(bucket, store, listversion, aws)
end

Base.show(io::IO,::S3Store) = print(io,"S3 Object Storage")

function error_is_ignorable(e)
  #isa(e,AWSException) && (e.code=="NoSuchKey" || e.code=="404")
  true
end

function Base.getindex(s::S3Store, i::String)
  try
    return S3.get_object(s.bucket,string(s.store,"/",i),aws_config=s.aws)
  catch e
    if error_is_ignorable(e)
      println(e)
      return nothing
    else
      throw(e)
    end
  end
end
getsub(s::S3Store, d::String) = S3Store(s.bucket, string(s.store,"/",d), s.listversion, s.aws)

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
    S3.head_object(s.bucket,string(s.store,"/",i),aws_config=s.aws)
    return true
  catch e
    if error_is_ignorable(e)
      return false
    else
      println(string(s.store,"/",i))
      throw(e)
    end
  end
end
Base.haskey(s::S3Store, i::String) = isinitialized(s,i)

function cloud_list_objects(s::S3Store)
  prefix = (isempty(s.store) || endswith(s.store,"/")) ? s.store : string(s.store,"/")
  listfun = s.listversion==2 ? S3.list_objects_v2 : S3.list_objects
  listfun(s.bucket, Dict("prefix"=>prefix, "delimiter" => "/"), aws_config = s.aws)
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

# Some special AWS configs
struct AnonymousGCS <:AbstractAWSConfig end
struct NoCredentials end
AWS.region(::AnonymousGCS) = "" # No region
AWS.credentials(::AnonymousGCS) = NoCredentials() # No credentials
AWS.check_credentials(c::NoCredentials) = c # Skip credentials check
AWS.sign!(::AnonymousGCS, ::AWS.Request) = nothing # Don't sign request
function AWS.generate_service_url(::AnonymousGCS, service::String, resource::String)
    service == "s3" || throw(ArgumentError("Can only handle s3 requests to GCS"))
    return string("https://storage.googleapis.com.", resource)
end

push!(storageregexlist,r"^gs://"=>AnonymousGCS)
push!(storageregexlist,r"^s3://"=>S3Store)

function storefromstring(::Type{<:S3Store}, s)
  decomp = split(s,"/",keepempty=false)
  bucket = decomp[2]
  path = join(decomp[3:end],"/")
  S3Store(String(bucket),path, aws=AWS.global_aws_config())
end

function storefromstring(::Type{<:AnonymousGCS}, s)
    decomp = split(s,"/",keepempty=false)
    bucket = decomp[2]
    path = join(decomp[3:end],"/")
    S3Store(String(bucket),path, aws=AnonymousGCS(), listversion=1)
end