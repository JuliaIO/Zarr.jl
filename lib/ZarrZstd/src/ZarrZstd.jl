"""
    ZarrZstd

Zstandard v2 compressor and v3 codec support.
"""
module ZarrZstd

import JSON # for JSON.lower

using ChunkCodecLibZstd: ZstdEncodeOptions
using ChunkCodecCore: encode, decode, decode!

# Qualify methods that extend ZarrCore or V3Codecs generics.
import ZarrCore
using ZarrCore: Compressor
using ZarrCore.Codecs: V3Codecs
using ZarrCore.Codecs.V3Codecs: V3Codec

# `reinterpret` requires a one-based, non-zero-dimensional array.
_reinterpret(::Type{T}, x::AbstractArray{S, 0}) where {T, S} = reinterpret(T, reshape(x, 1))
_reinterpret(::Type{T}, x::AbstractArray) where T = reinterpret(T, x)

# ## Zarr v2: `ZstdCompressor`

"""
    ZstdCompressor(;level=0, checksum=false)
Returns a `ZstdCompressor` struct that can serve as a Zarr array compressor. Keyword arguments are:
* `level=0`: the compression level, regular levels are 1 to 22, 0 is a special value for default, there are also even faster negative levels.
* `checksum=false`: flag to enable saving checksums.
"""
struct ZstdCompressor <: Compressor
    config::ZstdEncodeOptions
end

ZstdCompressor(;level=0, checksum::Bool=false) = ZstdCompressor(ZstdEncodeOptions(;compressionLevel=level, checksum))

function ZarrCore.getCompressor(::Type{ZstdCompressor}, d::Dict)
    ZstdCompressor(;
        level=get(Returns(0), d, "level"),
        checksum=Bool(get(Returns(false), d, "checksum")),
    )
end

# The numcodecs id that `ZstdCompressor` is stored under in a v2 `.zarray` document.
ZarrCore.codec_id(::Type{ZstdCompressor}) = "zstd"

# Statically typed counterpart of the `Dict` method above, for the juliac
# `--trim=safe` open path (`zopen(::Type{T}, ::Val{N}, ...)`). Every branch of
# an absent optional key returns the same concrete type - a ternary with
# differently typed branches would widen to `Any` under `--trim`.
function ZarrCore.getCompressor(::Type{ZstdCompressor}, c::ZarrCore.CompressorJSON)
    c.id == ZarrCore.codec_id(ZstdCompressor) || throw(ArgumentError(string(
        "expected a \"zstd\" compressor in the stored metadata, got \"", c.id, "\"")))
    lvl = 0
    if c.level !== nothing
        lvl = c.level::Int
    end
    chk = false
    if c.checksum !== nothing
        chk = c.checksum::Bool
    end
    return ZstdCompressor(; level=lvl, checksum=chk)
end

function ZarrCore.zuncompress(a, z::ZstdCompressor, ::Type{T}) where {T}
    result = decode(z.config.codec, a)
    _reinterpret(Base.nonmissingtype(T),result)
end

function ZarrCore.zuncompress!(data::DenseArray, compressed, z::ZstdCompressor)
    decode!(z.config.codec, reinterpret(UInt8, vec(data)), compressed)
    data
end

function ZarrCore.zcompress(a, z::ZstdCompressor)
    encode(z.config, reinterpret(UInt8, vec(a)))
end

function JSON.lower(z::ZstdCompressor)
    # Matching behavior in zarr-python to work with TensorStore
    # Ref https://github.com/JuliaIO/Zarr.jl/issues/193
    # Hotfix for https://github.com/zarr-developers/zarr-python/issues/2647
    # NamedTuples (not `Dict`s) so that serialising array metadata stays
    # statically typed - see `ZarrCore.print_metadata`. JSON output is unchanged.
    if z.config.checksum
        (; id = "zstd", level = z.config.compressionLevel, checksum = z.config.checksum)
    else
        (; id = "zstd", level = z.config.compressionLevel)
    end
end

function ZarrCore.v2_to_v3_codecs(z::ZstdCompressor, typesize::Int)
    (ZstdV3Codec(z.config.compressionLevel),)
end

# ## Zarr v3: `ZstdV3Codec`

"""
    ZstdV3Codec(level=3)

The zarr v3 `zstd` bytes->bytes codec, implemented by delegating to
[`ZstdCompressor`](@ref).
"""
struct ZstdV3Codec <: V3Codec{:bytes, :bytes}
    level::Int
end
ZstdV3Codec() = ZstdV3Codec(3)
V3Codecs.name(::ZstdV3Codec) = "zstd"

# The v3 spec name that `ZstdV3Codec` is stored under in a `zarr.json` document.
V3Codecs.codec_name(::Type{ZstdV3Codec}) = "zstd"

# Statically typed counterpart of the `register_codec` parser in `__init__`, for
# the juliac `--trim=safe` v3 open path
# (`zopen(::Type{T}, ::Val{N}, ...; pipeline = ...)`). Every branch of an absent
# optional key returns the same concrete type.
function V3Codecs.getCodec(::Type{ZstdV3Codec}, c::ZarrCore.CodecJSON, ctx)
    V3Codecs._check_codec_name(ZstdV3Codec, c)
    lvl = 3
    cfg = c.configuration
    if cfg !== nothing
        l = (cfg::ZarrCore.CodecConfigJSON).level
        if l !== nothing
            lvl = l::Int
        end
    end
    return ZstdV3Codec(lvl)
end

# NamedTuple (not `Dict`) so that lowering v3 array metadata stays statically
# typed - see `ZarrCore.print_metadata`. The emitted JSON is unchanged.
JSON.lower(c::ZstdV3Codec) = (; name = "zstd", configuration = (; level = c.level))

function V3Codecs.codec_encode(c::ZstdV3Codec, data::Vector{UInt8})
    comp = ZstdCompressor(level=c.level)
    return ZarrCore.zcompress(data, comp)
end

function V3Codecs.codec_decode(c::ZstdV3Codec, encoded::Vector{UInt8})
    comp = ZstdCompressor(level=c.level)
    return collect(ZarrCore.zuncompress(encoded, comp, UInt8))
end

# Cross-package registrations must run after precompilation.
function __init__()
    ZarrCore.compressortypes["zstd"] = ZstdCompressor
    V3Codecs.register_codec("zstd", ZstdV3Codec) do config, ctx
        ZstdV3Codec(get(config, "level", 3))
    end
end

@static if VERSION >= v"1.11"
    include("public_names_zstd.jl")
end

end # module
