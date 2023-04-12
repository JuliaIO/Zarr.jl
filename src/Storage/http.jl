using HTTP
using OpenSSL: OpenSSL

"""
    HTTPStore

A basic HTTP store without any credentials. The underlying data is supposed to be
consolidated and only read operations are supported. This store is compatible to
datasets being served through the [xpublish](https://xpublish.readthedocs.io/en/latest/)
python package.
"""
struct HTTPStore <: AbstractStore
    url::String
end

function Base.getindex(s::HTTPStore, k::String)
r = HTTP.request("GET",string(s.url,"/",k),status_exception = false,socket_type_tls=OpenSSL.SSLStream,connection_limit=25)
if r.status >= 300
    if r.status == 404
        nothing
    else
        error("Error connecting to $(s.url) :", String(r.body))
    end
else
    r.body
end
end


push!(storageregexlist,r"^https://"=>HTTPStore)
push!(storageregexlist,r"^http://"=>HTTPStore)
storefromstring(::Type{<:HTTPStore}, s,_) = ConsolidatedStore(HTTPStore(s),""),""

store_read_strategy(::HTTPStore) = ConcurrentRead(concurrent_io_tasks[])


## This is a server implementation for Zarr datasets
function zarr_req_handler(s::AbstractStore, p)
  if s[p,".zmetadata"] === nothing
    consolidate_metadata(s)
  end
  request -> begin
    k = request.target
    k = lstrip(k,'/')
    contains("..",k) && return nothing
    r = s[p,k]
    try
      if r ===  nothing
        return HTTP.Response(404, "Error: Key $k not found")
      else
        return HTTP.Response(200, r)
      end
    catch e
      return HTTP.Response(404, "Error: $e")
    end
  end
end


HTTP.serve(s::AbstractStore, p, args...; kwargs...) = HTTP.serve(zarr_req_handler(s,p),args...;kwargs...)
