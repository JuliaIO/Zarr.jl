function Zarr.S3Store(bucket::String;
    aws = nothing,
  )
  if aws === nothing
    aws = AWSS3.AWS.current_aws_config()
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

function Zarr.storagesize(s::S3Store,p)
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

function Zarr.isinitialized(s::S3Store, i::String)
  s3_exists(s.aws,s.bucket,i)
end


function Zarr.cloud_list_objects(s::S3Store,p)
  prefix = (isempty(p) || endswith(p,"/")) ? p : string(p,"/")
  s3_list_objects_delim(s.aws, s.bucket, prefix)
end
function s3_list_objects_delim(aws, bucket, prefix, delimiter="/")
    params = Dict("prefix" => prefix, "delimiter" => delimiter)
    result = Dict{String,Any}("CommonPrefixes" => [], "Contents" => [])
    while true
      resp = parse(AWSS3.S3.list_objects_v2(bucket, params; aws_config=aws))
      
      if haskey(resp, "CommonPrefixes")
        cp_ = isa(resp["CommonPrefixes"], Vector) ? resp["CommonPrefixes"] : [resp["CommonPrefixes"]]
        append!(result["CommonPrefixes"], cp_)
      end
      if haskey(resp, "Contents")
        ct_ = isa(resp["Contents"], Vector) ? resp["Contents"] : [resp["Contents"]]
        append!(result["Contents"], ct_)
      end
      if get(resp, "IsTruncated", "false") == "true"
        params["continuation-token"] = resp["NextContinuationToken"]
      else
        break
      end
    end
    result
end
function Zarr.subdirs(s::S3Store, p)
  s3_resp = cloud_list_objects(s, p)
  !haskey(s3_resp,"CommonPrefixes") && return String[]
  allstrings(s3_resp["CommonPrefixes"],"Prefix")
end
function Zarr.subkeys(s::S3Store, p)
  s3_resp = cloud_list_objects(s, p)
  !haskey(s3_resp,"Contents") && return String[]
  r = allstrings(s3_resp["Contents"],"Key")
  map(i->splitdir(i)[2],r)
end
allstrings(v::AbstractArray,prefixkey) = map(i -> rstrip(String(i[prefixkey]),'/'), v)
allstrings(v,prefixkey) = [rstrip(String(v[prefixkey]),'/')]

# push!(storageregexlist,r"^s3://"=>S3Store)

function Zarr.storefromstring(::Type{<:S3Store}, s, _)
  decomp = split(s,"/",keepempty=false)
  bucket = decomp[2]
  path = join(decomp[3:end],"/")
  S3Store(String(bucket),aws=AWSS3.AWS.current_aws_config()),path
end

Zarr.store_read_strategy(::S3Store) = ConcurrentRead(concurrent_io_tasks[])

function Zarr.zopen(s::S3Path, mode="r"; kwargs...)
  decomp = split(string(s),"/",keepempty=false)
  bucket = decomp[2]
  path = join(decomp[3:end],"/")
  store = S3Store(bucket, get_config(s))
  zopen(store, mode; path=path, kwargs...)
end