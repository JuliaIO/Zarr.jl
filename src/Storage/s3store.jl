using AWS

@service S3

struct S3Store <: AbstractStore
    bucket::String
    listversion::Int
    aws::AWS.AbstractAWSConfig
end


function S3Store(bucket::String;
  listversion = 2,
  aws = nothing,
  )
  if aws === nothing
    aws = AWS.global_aws_config()
  end
  S3Store(bucket, listversion, aws)
end

Base.show(io::IO,::S3Store) = print(io,"S3 Object Storage")

function error_is_ignorable(e)
  #isa(e,AWSException) && (e.code=="NoSuchKey" || e.code=="404")
  true
end

function Base.getindex(s::S3Store, i::String)
  try
    return S3.get_object(s.bucket,i,aws_config=s.aws)
  catch e
    if error_is_ignorable(e)
      return nothing
    else
      throw(e)
    end
  end
end

function Base.setindex!(s::S3Store, v, i::String)
  return S3.put_object(s.bucket,i,Dict("body"=>v),aws_config=s.aws)
end

Base.delete!(s::S3Store, d::String) = S3.delete_object(s.bucket,d, aws_config=s.aws)

function storagesize(s::S3Store,p)
  r = cloud_list_objects(s,p)
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

function isinitialized(s::S3Store, i::String)
  try
    S3.head_object(s.bucket,i,aws_config=s.aws)
    return true
  catch e
    if error_is_ignorable(e)
      return false
    else
      println(i)
      throw(e)
    end
  end
end

function cloud_list_objects(s::S3Store,p)
  prefix = (isempty(p) || endswith(p,"/")) ? p : string(p,"/")
  listfun = s.listversion==2 ? S3.list_objects_v2 : S3.list_objects
  listfun(s.bucket, Dict("prefix"=>prefix, "delimiter" => "/"), aws_config = s.aws)
end
function subdirs(s::S3Store, p)
  s3_resp = cloud_list_objects(s, p)
  !haskey(s3_resp,"CommonPrefixes") && return String[]
  allstrings(s3_resp["CommonPrefixes"],"Prefix")
end
function subkeys(s::S3Store, p)
  s3_resp = cloud_list_objects(s, p)
  !haskey(s3_resp,"Contents") && return String[]
  r = allstrings(s3_resp["Contents"],"Key")
  map(i->splitdir(i)[2],r)
end
allstrings(v::AbstractArray,prefixkey) = map(i -> rstrip(String(i[prefixkey]),'/'), v)
allstrings(v,prefixkey) = [rstrip(String(v[prefixkey]),'/')]

# Some special AWS configs
struct AnonymousGCS <:AbstractAWSConfig end
#struct NoCredentials end
#AWS.region(::AnonymousGCS) = "" # No region
AWS.credentials(::AnonymousGCS) = nothing # No credentials
#AWS.check_credentials(c::NoCredentials) = c # Skip credentials check
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
  S3Store(String(bucket),aws=AWS.global_aws_config()),path
end

function storefromstring(::Type{<:AnonymousGCS}, s)
    decomp = split(s,"/",keepempty=false)
    bucket = decomp[2]
    path = join(decomp[3:end],"/")
    S3Store(String(bucket), aws=AnonymousGCS(), listversion=1), path
end