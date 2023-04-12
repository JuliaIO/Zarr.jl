using URIs: URI

const GOOGLE_STORAGE_API = "https://storage.googleapis.com"
const GOOGLE_STORAGE_REST_API = GOOGLE_STORAGE_API * "/storage/v1"
const GOOGLE_STORAGE_CREDENTIALS = Dict{String,String}()

"""
    Zarr.gcs_credentials(user_project,access_token,token_type)

Set the user project, access token and and token type for the Google Cloud
Store.
"""
function gcs_credentials(user_project,access_token,token_type)
  GOOGLE_STORAGE_CREDENTIALS["user_project"] = user_project
  GOOGLE_STORAGE_CREDENTIALS["access_token"] = access_token
  GOOGLE_STORAGE_CREDENTIALS["token_type"] = token_type
  nothing
end

"""
    Zarr.gcs_credentials(; metadata_url = "http://metadata.google.internal/computeMetadata/v1/")

Set (or renew) the user project, access token and and token type for the Google
Cloud Store from the Meatadata server (assuming the function is executed from
the Google Cloud).
For some data sets, the error message "Bucket is requester pays bucket but no
user project provided" is returned if the credentials are not provided.
"""
function gcs_credentials(;metadata_url = "http://metadata.google.internal/computeMetadata/v1/")
  headers = Dict("Metadata-Flavor" => "Google")

  url = joinpath(metadata_url,"project","project-id")
  user_project = String(HTTP.get(url, headers=headers).body)

  url = joinpath(metadata_url,"instance","service-accounts","default","token")
  auth = JSON.parse(String(HTTP.get(url,headers).body));

  gcs_credentials(user_project,auth["access_token"],auth["token_type"])
end


function _gcs_request_headers()
  headers = Dict{String,String}()
  if haskey(GOOGLE_STORAGE_CREDENTIALS,"user_project")
    headers["x-goog-user-project"] = GOOGLE_STORAGE_CREDENTIALS["user_project"]
  end

  if haskey(GOOGLE_STORAGE_CREDENTIALS,"token_type") &&
    haskey(GOOGLE_STORAGE_CREDENTIALS,"access_token")

    headers["Authorization"] = string(
      GOOGLE_STORAGE_CREDENTIALS["token_type"]," ",
      GOOGLE_STORAGE_CREDENTIALS["access_token"])
  end

  return headers
end

struct GCStore <: AbstractStore
  bucket::String

  function GCStore(url::String)
    uri = URI(url)

    if uri.scheme == "gs"
      bucket = uri.host
    else
      parts = split(uri.path,'/',limit=3)
      bucket = parts[2]
    end
    @debug "GCS bucket: $bucket"
    new(bucket)
  end
end


function Base.getindex(s::GCStore, k::String)
  url = string(GOOGLE_STORAGE_API,"/",s.bucket,"/",k)
  headers = _gcs_request_headers()
  r = HTTP.request("GET",url,headers,status_exception = false)
  if r.status >= 300
    if r.status == 404
      @debug "get: $url: not found"
      nothing
    else
      error("Error connecting to $url :", String(r.body))
    end
  else
    @debug "get: $url"
    r.body
  end
end

function cloud_list_objects(s::GCStore,p)
  prefix = (isempty(p) || endswith(p,"/")) ? p : string(p,"/")

  url = string(GOOGLE_STORAGE_REST_API, "/b/", s.bucket, "/o")

  @debug "call: $url"
  headers = _gcs_request_headers()
  params = Dict("prefix" => prefix, "delimiter" => "/")
  r = JSON.parse(String(HTTP.get(url,headers,
                                 query = params).body))

  return r
end

function storagesize(s::GCStore,p)
  r = cloud_list_objects(s,p)
  items = r["items"]
  datafiles = filter(entry -> !any(filename -> endswith(entry["name"], filename), [".zattrs",".zarray",".zgroup"]), items)
  if isempty(datafiles)
    0
  else
    sum(datafiles) do f
      parse(Int, f["size"])
    end
  end
end

function subkeys(s::GCStore, p)
  r = cloud_list_objects(s, p)
  keys = map(item -> String(split(item["name"],'/')[end]),  r["items"])
  return keys
end

function subdirs(s::GCStore, p)
  r = cloud_list_objects(s,p)
  dirs = map(prefix -> String(split(prefix,'/')[end-1]), r["prefixes"])
  return dirs
end

pushfirst!(storageregexlist,r"^https://storage.googleapis.com"=>GCStore)
pushfirst!(storageregexlist,r"^http://storage.googleapis.com"=>GCStore)
push!(storageregexlist,r"^gs://"=>GCStore)

function storefromstring(::Type{<:GCStore}, url,_)
  uri = URI(url)
  if uri.scheme == "gs"
    p = lstrip(uri.path,'/')
  else
    parts = split(uri.path,'/',limit=2, keepempty=false)
    p = (length(parts) == 2 ? parts[2] : "")
  end

  @debug "path: $p"
  return GCStore(url),p
end

store_read_strategy(::GCStore) = ConcurrentRead(concurrent_io_tasks[])