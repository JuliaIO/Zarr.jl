using HTTP
using OpenSSL: OpenSSL

"""
    HTTPStore

A basic HTTP store without any credentials. The underlying data is supposed to be
consolidated and only read operations are supported. This store is compatible to
datasets being served through the [xpublish](https://xpublish.readthedocs.io/en/latest/)
python package. In case you experience performance options, one can try to use 
`HTTP.set_default_connection_limit!` to increase the number of concurrent connections. 
"""
struct HTTPStore <: AbstractStore
    url::String
    allowed_codes::Set{Int}
end
HTTPStore(url) = HTTPStore(url,Set((404,)))

function Base.getindex(s::HTTPStore, k::String)
r = HTTP.request("GET",string(s.url,"/",k),status_exception = false,socket_type_tls=OpenSSL.SSLStream)
if r.status >= 300
    if r.status in s.allowed_codes
        nothing
    else
        err_msg = 
        """Received error code $(r.status) when connecting to $(s.url) with message $(String(r.body)).
        This might be an actual error or an indication that the server returns a different error code 
        than 404 for missing chunks. In the later case you can run 
        `Zarr.missing_chunk_return_code!(a.storage,$(r.status))` where a is your Zarr array or group to
        fix the issue.
        """
        throw(ErrorException(err_msg))
    end
else
    r.body
end
end


push!(storageregexlist,r"^https://"=>HTTPStore)
push!(storageregexlist,r"^http://"=>HTTPStore)
storefromstring(::Type{<:HTTPStore}, s,_) = ConsolidatedStore(HTTPStore(s),""),""

"""
    missing_chunk_return_code!(s::HTTPStore, code::Union{Int,AbstractVector{Int}})

Extends the list of HTTP return codes that signals that a certain key in a HTTPStore is not available. Most data providers
return code 404 for missing elements, but some may use different return codes like 403. This function can be used
to add return codes that signal missing chunks. 

### Example

````julia
a = zopen("https://path/to/remote/array")
missing_chunk_return_code!(a.storage, 403)
````
"""
missing_chunk_return_code!(s::ConsolidatedStore,code) = missing_chunk_return_code!(s.parent,code)
missing_chunk_return_code!(s::HTTPStore, code::Integer) = push!(s.allowed_codes,code)
missing_chunk_return_code!(s::HTTPStore, codes::AbstractVector{<:Integer}) = foreach(c->push!(s.allowed_codes,c),codes)
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
