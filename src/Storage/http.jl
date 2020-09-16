using HTTP

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
r = HTTP.request("GET",string(s.url,"/",k),status_exception = false)
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
getsub(s::HTTPStore,n) = HTTPStore(string(s.url,"/",n))
zname(s::HTTPStore) = split(s.url,"/")[end]


## This is a server implementation for Zarr datasets




function zarr_req_handler(s::AbstractStore)
  if s[".zmetadata"] === nothing
    consolidate_metadata(s)
  end
  request -> begin
    k = request.target
    while startswith(k,"/")
      k = k[2:end]
    end
    k_split = filter(!isequal(".."),split(k,"/"))
    storenew = if length(k_split)>1
      foldl((ss,kk)->getsub(ss,kk),k_split[1:end-1],init = s)
    else
      s
    end
    # @show zname(storenew), k_split
    # @show keys(storenew), k_split[end]
    r = storenew[k_split[end]]

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


HTTP.serve(s::AbstractStore, args...; kwargs...) = HTTP.serve(zarr_req_handler(s),args...;kwargs...)
