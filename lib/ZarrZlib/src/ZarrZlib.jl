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

# The numcodecs id that `ZlibCompressor` is stored under in a v2 `.zarray` document.
ZarrCore.codec_id(::Type{ZlibCompressor}) = "zlib"

# Statically typed counterpart of the `Dict` method above, for the juliac
# `--trim=safe` open path (`zopen(::Type{T}, ::Val{N}, ...)`). numcodecs spells
# the zlib compression level `level`; `clevel` is accepted as an alias and a
# missing key falls back to `ZlibCompressor`'s own default of -1 (the `Dict`
# method has no default because `.zarray` documents written by zarr-python and
# by Zarr.jl always carry the key). Both branches of every optional key return
# the same concrete type.
function ZarrCore.getCompressor(::Type{ZlibCompressor}, c::ZarrCore.CompressorJSON)
    c.id == ZarrCore.codec_id(ZlibCompressor) || throw(ArgumentError(string(
        "expected a \"zlib\" compressor in the stored metadata, got \"", c.id, "\"")))
    lvl = -1
    if c.level !== nothing
        lvl = c.level::Int
    elseif c.clevel !== nothing
        lvl = c.clevel::Int
    end
    return ZlibCompressor(lvl)
end

function ZarrCore.zuncompress(a, z::ZlibCompressor, ::Type{T}) where {T}
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

# NamedTuple (not `Dict`) so that serialising array metadata stays statically
# typed - see `ZarrCore.print_metadata`. The emitted JSON is unchanged.
JSON.lower(z::ZlibCompressor) = (; id = "zlib", level = z.config.level)

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

# The v3 spec name that `GzipV3Codec` is stored under in a `zarr.json` document.
V3Codecs.codec_name(::Type{GzipV3Codec}) = "gzip"

# Statically typed counterpart of the `register_codec` parser in `__init__`, for
# the juliac `--trim=safe` v3 open path
# (`zopen(::Type{T}, ::Val{N}, ...; pipeline = ...)`). Every branch of an absent
# optional key returns the same concrete type.
function V3Codecs.getCodec(::Type{GzipV3Codec}, c::ZarrCore.CodecJSON, ctx)
    V3Codecs._check_codec_name(GzipV3Codec, c)
    lvl = 6
    cfg = c.configuration
    if cfg !== nothing
        l = (cfg::ZarrCore.CodecConfigJSON).level
        if l !== nothing
            lvl = l::Int
        end
    end
    return GzipV3Codec(lvl)
end

# NamedTuple (not `Dict`) so that lowering v3 array metadata stays statically
# typed - see `ZarrCore.print_metadata`. The emitted JSON is unchanged.
JSON.lower(c::GzipV3Codec) = (; name = "gzip", configuration = (; level = c.level))

function V3Codecs.codec_encode(c::GzipV3Codec, data::Vector{UInt8})
    opts = GzipEncodeOptions(; level=c.level)
    return encode(opts, data)
end

function V3Codecs.codec_decode(c::GzipV3Codec, encoded::Vector{UInt8})
    return decode(GzipCodec(), encoded)
end

# Cross-package registrations must run after precompilation.
function __init__()
    ZarrCore.compressortypes["zlib"] = ZlibCompressor
    V3Codecs.register_codec("gzip", GzipV3Codec) do config, ctx
        GzipV3Codec(get(config, "level", 6))
    end
end

@static if VERSION >= v"1.11"
    include("public_names_zlib.jl")
end

end # module
