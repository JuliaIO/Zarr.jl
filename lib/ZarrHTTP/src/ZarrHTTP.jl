"""
    ZarrHTTP

HTTP support for Zarr.jl: the read-only [`HTTPStore`](@ref), which serves a
consolidated Zarr dataset over plain HTTP(S), and the small server that goes the
other way and exposes any `AbstractStore` (or `ZArray`/`ZGroup`) through
`HTTP.serve`.

This is a subpackage of Zarr.jl; its public API is re-exported by `Zarr`, so
`Zarr.HTTPStore` and `zopen("https://...")` keep working exactly as before.
"""
module ZarrHTTP

using HTTP: HTTP
using OpenSSL: OpenSSL

# Only the names that are used unqualified live here. Methods that *extend* a
# ZarrCore generic are always written as `ZarrCore.f(...)` below: writing a bare
# `f(...)` definition would silently create a new `ZarrHTTP.f` that shadows the
# generic instead of adding a method to it, and `zopen` would then never see it.
import ZarrCore
using ZarrCore: @public, AbstractStore, ZArray, ZGroup, ConsolidatedStore,
    ConcurrentRead, concurrent_io_tasks, consolidate_metadata, storageregexlist

"""
    PUBLIC_NAMES::Vector{Symbol}

Every name this module declares with `ZarrCore.@public`, in declaration order.
See `ZarrCore.PUBLIC_NAMES` -- this registry is per-module and is what lets the
`Zarr` facade re-export the public API on Julia 1.10, which has no `public`.
"""
const PUBLIC_NAMES = Symbol[]

"""
    HTTPStore

A basic HTTP store without any credentials. The underlying data is supposed to be
consolidated and only read operations are supported. This store is compatible to
datasets being served through the [xpublish](https://xpublish.readthedocs.io/en/latest/)
python package. In case you experience performance issues, one can try to use
`HTTP.set_default_connection_limit!` to increase the number of concurrent connections.
"""
struct HTTPStore <: AbstractStore
    url::String
    allowed_codes::Set{Int}
    HTTPStore(url, allowed_codes = Set((404,))) = new(url, allowed_codes)
end

Base.show(io::IO, ::HTTPStore) = print(io, "HTTP Storage")

function Base.getindex(s::HTTPStore, k::String)
    r = HTTP.request("GET", string(s.url, "/", k), status_exception = false, socket_type_tls = OpenSSL.SSLStream)
    if r.status >= 300
        if r.status in s.allowed_codes
            nothing
        else
            err_msg =
            """Received error code $(r.status) when connecting to $(s.url) with message $(String(r.body)).
            This might be an actual error, or an indication that the server returns a different error code
            than 404 for missing chunks. In the latter case, you can run
            `ZarrCore.missing_chunk_return_code!(a.storage,$(r.status))` where `a` is your Zarr array or group,
            to fix the issue.
            """
            throw(ErrorException(err_msg))
        end
    else
        r.body
    end
end

function ZarrCore.storefromstring(::Type{<:HTTPStore}, s, _)
    http_store = HTTPStore(s)
    try
        cs = ConsolidatedStore(http_store, "")
        return cs, ""
    catch err
        @warn exception=err "Additional metadata was not available for HTTPStore."
    end
    return http_store,""
end

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
ZarrCore.missing_chunk_return_code!(s::HTTPStore, code::Integer) = push!(s.allowed_codes,code)
ZarrCore.missing_chunk_return_code!(s::HTTPStore, codes::AbstractVector{<:Integer}) = foreach(c->push!(s.allowed_codes,c),codes)
ZarrCore.store_read_strategy(::HTTPStore) = ConcurrentRead(concurrent_io_tasks[])
ZarrCore.has_configurable_missing_chunks(::HTTPStore) = true


## This is a server implementation for Zarr datasets
function zarr_req_handler(s::AbstractStore, p, notfound = 404)
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
        return HTTP.Response(notfound, "Error: Key $k not found")
      else
        return HTTP.Response(200, r)
      end
    catch e
      return HTTP.Response(notfound, "Error: $e")
    end
  end
end


HTTP.serve(s::AbstractStore, p, args...; kwargs...) = HTTP.serve(zarr_req_handler(s,p),args...;kwargs...)
HTTP.serve(s::Union{ZArray,ZGroup}, args...; kwargs...) = HTTP.serve(s.storage, s.path, args...; kwargs...)

# The registry lives in `ZarrCore`, so the entries have to be added at *load*
# time, not at precompile time: a mutation of another package's global state
# made while this module's body runs is discarded when the precompiled image is
# written out, and the entry would simply be missing in every fresh session.
#
# `storageregexlist` keeps itself sorted most-specific-first, so these generic
# patterns lose to the host-qualified ones registered by e.g. `ZarrGCS`
# regardless of which package is loaded first.
function __init__()
    push!(storageregexlist, r"^https://" => HTTPStore)
    push!(storageregexlist, r"^http://" => HTTPStore)
end

@public HTTPStore

end # module
