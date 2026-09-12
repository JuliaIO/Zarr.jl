"""
    ZarrZlib

Zlib v2 compressor and gzip v3 codec support.
"""
module ZarrZlib

import JSON # for JSON.lower

using ChunkCodecLibZlib: ZlibEncodeOptions, GzipCodec, GzipEncodeOptions
using ChunkCodecCore: encode, decode, decode!

# Qualify methods that extend ZarrCore or V3Codecs generics.
import ZarrCore
using ZarrCore: Compressor
using ZarrCore.Codecs: V3Codecs
using ZarrCore.Codecs.V3Codecs: V3Codec

# `reinterpret` requires a one-based, non-zero-dimensional array.
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

# Cross-package registrations must run after precompilation.
function __init__()
    ZarrCore.should_register_at_init() && register!()
end

"""
    ZarrZlib.register!()

Register the zlib compressor with ZarrCore under the Zarr v2 compressor name
`"zlib"` and the Zarr v3 codec name `"gzip"`.

Registration runs automatically during package initialization when the
ZarrCore `RegisterAtInit` preference is enabled (the default). When automatic
registration is disabled, call this function explicitly. Calling it again
restores or overwrites ZarrZlib's own registry entries.
"""
function register!()
    ZarrCore.compressortypes["zlib"] = ZlibCompressor
    V3Codecs.register_codec("gzip", GzipV3Codec) do config, ctx
        GzipV3Codec(get(config, "level", 6))
    end
end

@static if VERSION >= v"1.11"
    include("public_names_zlib.jl")
end

end # module
