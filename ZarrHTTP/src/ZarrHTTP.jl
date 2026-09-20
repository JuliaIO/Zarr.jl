"""
    ZarrHTTP

HTTP storage and serving support.
"""
module ZarrHTTP

using HTTP: HTTP

# Qualify methods that extend ZarrCore generics.
import ZarrCore
using ZarrCore: AbstractStore, ZArray, ZGroup, ConsolidatedStore, DirectoryStore, CachingStore,
    ConcurrentRead, concurrent_io_tasks, consolidate_metadata, storageregexlist

"""
    HTTPStore

Read-only HTTP storage. Consolidated metadata is used when available.
"""
struct HTTPStore <: AbstractStore
    url::String
    allowed_codes::Set{Int}
    HTTPStore(url, allowed_codes = Set((404,))) = new(url, allowed_codes)
end

Base.show(io::IO, ::HTTPStore) = print(io, "HTTP Storage")

function Base.getindex(s::HTTPStore, k::String)
    r = HTTP.request("GET", string(s.url, "/", k), status_exception = false)
    if r.status >= 300
        if r.status in s.allowed_codes
            nothing
        else
            err_msg = "Received error code $(r.status) from $(s.url): $(String(r.body)). " *
                "If this code means a missing chunk, register it with " *
                "ZarrCore.missing_chunk_return_code!(store, $(r.status))."
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
        @warn exception=err "Could not load consolidated HTTP metadata"
    end
    return http_store,""
end

"""
    missing_chunk_return_code!(s::HTTPStore, code::Union{Int,AbstractVector{Int}})

Treat `code` as a missing-key response for `s`.
"""
ZarrCore.missing_chunk_return_code!(s::HTTPStore, code::Integer) = push!(s.allowed_codes,code)
ZarrCore.missing_chunk_return_code!(s::HTTPStore, codes::AbstractVector{<:Integer}) = foreach(c->push!(s.allowed_codes,c),codes)
ZarrCore.store_read_strategy(::HTTPStore) = ConcurrentRead(concurrent_io_tasks[])
ZarrCore.has_configurable_missing_chunks(::HTTPStore) = true


# Reason to refuse serving key `k` below path `p` of store `s`, or `nothing`.
function invalid_key_reason(s::AbstractStore, p, k)
  if ".." in split(k, ('/','\\'))
    return "keys must not contain \"..\" path segments, since they could address data outside the served path"
  end
  outside_store_reason(s, p, k)
end

# Store-specific check that the key stays inside what the store serves.
outside_store_reason(::AbstractStore, p, k) = nothing
function outside_store_reason(s::DirectoryStore, p, k)
  # Resolve the file the store would read. This also catches keys that
  # `joinpath` treats as absolute, such as "C:/secret.txt" on Windows.
  root = abspath(joinpath(s.folder, p))
  rel = relpath(abspath(joinpath(s.folder, ZarrCore._concatpath(p, k))), root)
  (isabspath(rel) || first(splitpath(rel)) == "..") || return nothing
  "the key resolves to a file outside the served directory"
end
outside_store_reason(s::ConsolidatedStore, p, k) = outside_store_reason(s.parent, p, k)
function outside_store_reason(s::CachingStore, p, k)
  reason = outside_store_reason(s.cache, p, k)
  reason === nothing ? outside_store_reason(s.remote, p, k) : reason
end

# Serve a store through HTTP.
function zarr_req_handler(s::AbstractStore, p, notfound = 404)
  if s[p,".zmetadata"] === nothing
    consolidate_metadata(s)
  end
  request -> begin
    k = request.target
    # Stores such as DirectoryStore only index by String, not SubString.
    k = String(lstrip(k,'/'))
    reason = invalid_key_reason(s, p, k)
    if reason !== nothing
      return HTTP.Response(400, "Error: Invalid request target \"$(request.target)\": $reason")
    end
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
HTTP.serve!(s::AbstractStore, p::AbstractString, args...; kwargs...) = HTTP.serve!(zarr_req_handler(s,p), args...; kwargs...)
HTTP.serve!(s::AbstractStore, p::AbstractString, host::AbstractString, port_num::Integer; kwargs...) = HTTP.serve!(zarr_req_handler(s,p), host, port_num; kwargs...)
HTTP.serve!(s::AbstractStore, p::AbstractString, host::AbstractString; kwargs...) = HTTP.serve!(zarr_req_handler(s,p), host; kwargs...)
HTTP.serve!(s::AbstractStore, p::AbstractString, port_num::Integer; kwargs...) = HTTP.serve!(zarr_req_handler(s,p), port_num; kwargs...)

HTTP.serve(s::Union{ZArray,ZGroup}, args...; kwargs...) = HTTP.serve(s.storage, s.path, args...; kwargs...)
HTTP.serve!(s::Union{ZArray,ZGroup}, host::AbstractString, port_num::Integer; kwargs...) = HTTP.serve!(s.storage, s.path, host, port_num; kwargs...)
HTTP.serve!(s::Union{ZArray,ZGroup}, host::AbstractString; kwargs...) = HTTP.serve!(s.storage, s.path, host; kwargs...)
HTTP.serve!(s::Union{ZArray,ZGroup}, port_num::Integer; kwargs...) = HTTP.serve!(s.storage, s.path, port_num; kwargs...)
HTTP.serve!(s::Union{ZArray,ZGroup}; kwargs...) = HTTP.serve!(s.storage, s.path; kwargs...)

# Register after precompilation; specific URL patterns take precedence.
function __init__()
    ZarrCore.should_register_at_init() && register!()
end

"""
    ZarrHTTP.register!()

Register the `https://` and `http://` URL schemes with ZarrCore.
Registration runs automatically by default according to the ZarrCore
`RegisterAtInit` preference. Call `ZarrHTTP.register!()` explicitly when
automatic registration is disabled.
"""
function register!()
    push!(storageregexlist, r"^https://" => HTTPStore)
    push!(storageregexlist, r"^http://" => HTTPStore)
end

@static if VERSION >= v"1.11"
    include("public_names_http.jl")
end

end # module
