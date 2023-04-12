using AWSS3: AWSS3, s3_put, s3_get, s3_delete, s3_list_objects, s3_exists

struct S3Store <: AbstractStore
    bucket::String
    aws::AWSS3.AWS.AbstractAWSConfig
end


function S3Store(bucket::String;
    aws = nothing,
  )
  if aws === nothing
    aws = AWSS3.AWS.global_aws_config()
  end
  S3Store(bucket, aws)
end

Base.show(io::IO,::S3Store) = print(io,"S3 Object Storage")

function Base.getindex(s::S3Store, i::String)
  try
    return s3_get(s.aws,s.bucket,i,raw=true,retry=false)
  catch e
    if e isa AWSS3.AWS.AWSException && e.code == "NoSuchKey"
      return nothing
    else
      throw(e)
    end
  end
end

function Base.setindex!(s::S3Store, v, i::String)
  return s3_put(s.aws,s.bucket,i,v)
end

Base.delete!(s::S3Store, d::String) = s3_delete(s.aws,s.bucket,d)

function storagesize(s::S3Store,p)
  prefix = (isempty(p) || endswith(p,"/")) ? p : string(p,"/")
  r = s3_list_objects(s.aws,s.bucket,prefix)
  s = 0
  for entry in r
    filename = splitdir(entry["Key"])[2]
    if !in(filename,(".zattrs",".zarray",".zgroup"))
      s = s+parse(Int,entry["Size"])
    end
  end
  s
end

function isinitialized(s::S3Store, i::String)
  s3_exists(s.aws,s.bucket,i)
end


function cloud_list_objects(s::S3Store,p)
  prefix = (isempty(p) || endswith(p,"/")) ? p : string(p,"/")
  AWSS3.S3.list_objects_v2(s.bucket, Dict("prefix"=>prefix, "delimiter" => "/"), aws_config = s.aws)
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

push!(storageregexlist,r"^s3://"=>S3Store)

function storefromstring(::Type{<:S3Store}, s, _)
  decomp = split(s,"/",keepempty=false)
  bucket = decomp[2]
  path = join(decomp[3:end],"/")
  S3Store(String(bucket),aws=AWSS3.AWS.global_aws_config()),path
end

store_read_strategy(::S3Store) = ConcurrentRead(concurrent_io_tasks[])
