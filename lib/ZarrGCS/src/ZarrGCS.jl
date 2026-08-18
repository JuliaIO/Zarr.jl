"""
    ZarrGCS

Google Cloud Storage support for Zarr.jl: the read-only [`GCStore`](@ref), which
talks to the GCS JSON/XML APIs over plain HTTP, plus
[`gcs_credentials`](@ref) for requester-pays and private buckets.

This package depends on HTTP.jl directly rather than on `ZarrHTTP`: it issues
its own requests and never goes through `HTTPStore`.

This is a subpackage of Zarr.jl; its public API is re-exported by `Zarr`, so
`Zarr.GCStore` and `zopen("gs://...")` keep working exactly as before.
"""
module ZarrGCS

using HTTP: HTTP
using URIs: URI
import JSON

# Only the names that are used unqualified live here. Methods that *extend* a
# ZarrCore generic are always written as `ZarrCore.f(...)` below: writing a bare
# `f(...)` definition would silently create a new `ZarrGCS.f` that shadows the
# generic instead of adding a method to it, and `zopen` would then never see it.
import ZarrCore
using ZarrCore: @public, AbstractStore, ConcurrentRead, concurrent_io_tasks,
    storageregexlist

"""
    PUBLIC_NAMES::Vector{Symbol}

Every name this module declares with `ZarrCore.@public`, in declaration order.
See `ZarrCore.PUBLIC_NAMES` -- this registry is per-module and is what lets the
`Zarr` facade re-export the public API on Julia 1.10, which has no `public`.
"""
const PUBLIC_NAMES = Symbol[]

const GOOGLE_STORAGE_API = "https://storage.googleapis.com"
const GOOGLE_STORAGE_REST_API = GOOGLE_STORAGE_API * "/storage/v1"
const GOOGLE_STORAGE_CREDENTIALS = Dict{String,String}()

"""
    gcs_credentials(user_project,access_token,token_type)

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
    gcs_credentials(; metadata_url = "http://metadata.google.internal/computeMetadata/v1/")

Set (or renew) the user project, access token and and token type for the Google
Cloud Store from the Metadata server (assuming the function is executed from
the Google Cloud).
For some data sets, the error message "Bucket is requester pays bucket but no
user project provided" is returned if the credentials are not provided.
"""
function gcs_credentials(;metadata_url = "http://metadata.google.internal/computeMetadata/v1/")
  headers = Dict("Metadata-Flavor" => "Google")

  url = joinpath(metadata_url,"project","project-id")
  user_project = String(HTTP.get(url, headers=headers).body)

  url = joinpath(metadata_url,"instance","service-accounts","default","token")
  auth = JSON.parse(String(HTTP.get(url,headers).body); dicttype=Dict);

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

"""
    GCStore(url::String)

A read-only store for a Google Cloud Storage bucket. `url` may be either a
`gs://bucket/path` URL or an `https://storage.googleapis.com/bucket/path` one.
"""
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

function ZarrCore.cloud_list_objects(s::GCStore,p)
  prefix = (isempty(p) || endswith(p,"/")) ? p : string(p,"/")

  url = string(GOOGLE_STORAGE_REST_API, "/b/", s.bucket, "/o")

  @debug "call: $url"
  headers = _gcs_request_headers()
  params = Dict("prefix" => prefix, "delimiter" => "/")
  r = JSON.parse(String(HTTP.get(url,headers,
                                 query = params).body); dicttype=Dict)

  return r
end

function ZarrCore.storagesize(s::GCStore,p)
  r = ZarrCore.cloud_list_objects(s,p)
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

function ZarrCore.subkeys(s::GCStore, p)
  r = ZarrCore.cloud_list_objects(s, p)
  keys = map(item -> String(split(item["name"],'/')[end]),  r["items"])
  return keys
end

function ZarrCore.subdirs(s::GCStore, p)
  r = ZarrCore.cloud_list_objects(s,p)
  dirs = map(prefix -> String(split(prefix,'/')[end-1]), r["prefixes"])
  return dirs
end

function ZarrCore.storefromstring(::Type{<:GCStore}, url,_)
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

ZarrCore.store_read_strategy(::GCStore) = ConcurrentRead(concurrent_io_tasks[])

# The registry lives in `ZarrCore`, so the entries have to be added at *load*
# time, not at precompile time: a mutation of another package's global state
# made while this module's body runs is discarded when the precompiled image is
# written out, and the entry would simply be missing in every fresh session.
#
# The first two patterns also match `ZarrHTTP`'s generic `^https?://` patterns.
# `storageregexlist` sorts by specificity, so the host-qualified patterns below
# win regardless of whether the HTTP backend registered before or after this one.
function __init__()
  push!(storageregexlist, r"^https://storage.googleapis.com" => GCStore)
  push!(storageregexlist, r"^http://storage.googleapis.com" => GCStore)
  push!(storageregexlist, r"^gs://" => GCStore)
end

# `GCStore` was exported by `ZarrCore` before it moved here, so it is exported
# (not just public) to keep `using Zarr; GCStore` working.
export GCStore
@public gcs_credentials

end # module
