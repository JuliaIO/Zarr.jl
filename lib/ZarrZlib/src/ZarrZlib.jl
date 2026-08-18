"""
    ZarrZlib

Zlib/gzip support for Zarr.jl: the zarr v2 [`ZlibCompressor`](@ref) and the
matching zarr v3 [`GzipV3Codec`](@ref), both backed by ChunkCodecLibZlib.jl.

The two names differ because the specs do: zarr v2 calls the numcodecs id
`"zlib"`, while zarr v3 calls the codec `"gzip"`.

This is a subpackage of Zarr.jl; its public API is re-exported by `Zarr`, so
`Zarr.ZlibCompressor` keeps working exactly as before.
"""
module ZarrZlib

import JSON # for JSON.lower

using ChunkCodecLibZlib: ZlibEncodeOptions, GzipCodec, GzipEncodeOptions
using ChunkCodecCore: encode, decode, decode!

# Only the names that are used unqualified live here. Methods that *extend* a
# ZarrCore generic are always written as `ZarrCore.f(...)` (or
# `V3Codecs.f(...)`) below: writing a bare `f(...)` definition would silently
# create a new `ZarrZlib.f` that shadows the generic instead of adding a method
# to it, and nothing in ZarrCore would ever see it.
import ZarrCore
using ZarrCore: @public, Compressor
using ZarrCore.Codecs: V3Codecs
using ZarrCore.Codecs.V3Codecs: V3Codec

"""
    PUBLIC_NAMES::Vector{Symbol}

Every name this module declares with `ZarrCore.@public`, in declaration order.
See `ZarrCore.PUBLIC_NAMES` -- this registry is per-module and is what lets the
`Zarr` facade re-export the public API on Julia 1.10, which has no `public`.
"""
const PUBLIC_NAMES = Symbol[]

# `reinterpret` needs a 1-based, non-zero-dimensional array; a 0-d chunk has to
# be reshaped first. Kept local rather than shared, so that this package depends
# only on ZarrCore's documented API.
_reinterpret(::Type{T}, x::AbstractArray{S, 0}) where {T, S} = reinterpret(T, reshape(x, 1))
_reinterpret(::Type{T}, x::AbstractArray) where T = reinterpret(T, x)

# ## Zarr v2: `ZlibCompressor`

"""
    ZlibCompressor(clevel=-1)
Returns a `ZlibCompressor` struct that can serve as a Zarr array compressor. Keyword arguments are:
* `clevel=-1` the compression level, number between -1 (Default), 0 (no compression) and 9 (max compression)
*  default is -1 compromise between speed and compression (currently equivalent to level 6).
"""
struct ZlibCompressor <: Compressor
    config::ZlibEncodeOptions
end

ZlibCompressor(clevel::Integer) = ZlibCompressor(ZlibEncodeOptions(;level=clevel))

ZlibCompressor(;clevel=-1) = ZlibCompressor(clevel)

function ZarrCore.getCompressor(::Type{ZlibCompressor}, d::Dict)
    ZlibCompressor(d["level"])
end

function ZarrCore.zuncompress(a, z::ZlibCompressor, T)
    result = decode(z.config.codec, a)
    _reinterpret(Base.nonmissingtype(T),result)
end

function ZarrCore.zuncompress!(data::DenseArray, compressed, z::ZlibCompressor)
    decode!(z.config.codec, reinterpret(UInt8, vec(data)), compressed)
    data
end

function ZarrCore.zcompress(a, z::ZlibCompressor)
    encode(z.config, reinterpret(UInt8, vec(a)))
end

JSON.lower(z::ZlibCompressor) = Dict("id"=>"zlib", "level" => z.config.level)

function ZarrCore.v2_to_v3_codecs(z::ZlibCompressor, typesize::Int)
    # ZlibCompressor uses -1 to mean "default"; zarr v3 gzip spec requires 0-9
    level = z.config.level == -1 ? 6 : z.config.level
    (GzipV3Codec(level),)
end

# ## Zarr v3: `GzipV3Codec`

"""
    GzipV3Codec(level=6)

The zarr v3 `gzip` bytes->bytes codec. This is the v3 spelling of what zarr v2
calls the `zlib` compressor, see [`ZlibCompressor`](@ref).
"""
struct GzipV3Codec <: V3Codec{:bytes, :bytes}
    level::Int
end
GzipV3Codec() = GzipV3Codec(6)
V3Codecs.name(::GzipV3Codec) = "gzip"

function JSON.lower(c::GzipV3Codec)
    Dict("name" => "gzip", "configuration" => Dict("level" => c.level))
end

function V3Codecs.codec_encode(c::GzipV3Codec, data::Vector{UInt8})
    opts = GzipEncodeOptions(; level=c.level)
    return encode(opts, data)
end

function V3Codecs.codec_decode(c::GzipV3Codec, encoded::Vector{UInt8})
    return decode(GzipCodec(), encoded)
end

# Both registries live in `ZarrCore`, so the entries have to be added at *load*
# time, not at precompile time: a mutation of another package's global state
# made while this module's body runs is discarded when the precompiled image is
# written out, and the entry would simply be missing in every fresh session.
function __init__()
    ZarrCore.compressortypes["zlib"] = ZlibCompressor
    V3Codecs.register_codec("gzip", GzipV3Codec) do config, ctx
        GzipV3Codec(get(config, "level", 6))
    end
end

@public ZlibCompressor, GzipV3Codec

end # module
