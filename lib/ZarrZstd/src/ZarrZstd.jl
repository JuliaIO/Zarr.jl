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

function ZarrCore.zuncompress(a, z::ZstdCompressor, T)
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
    if z.config.checksum
        Dict("id"=>"zstd", "level" => z.config.compressionLevel, "checksum" => z.config.checksum)
    else
        Dict("id"=>"zstd", "level" => z.config.compressionLevel)
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

function JSON.lower(c::ZstdV3Codec)
    Dict("name" => "zstd", "configuration" => Dict("level" => c.level))
end

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
    ZarrCore.should_register_at_init() && register!()
end

"""
    ZarrZstd.register!()

Register the Zstandard compressor with ZarrCore under the Zarr v2 compressor
name `"zstd"` and the Zarr v3 codec name `"zstd"`.

Registration runs automatically during package initialization when the
ZarrCore `RegisterAtInit` preference is enabled (the default). When automatic
registration is disabled, call this function explicitly. Calling it again
restores or overwrites ZarrZstd's own registry entries.
"""
function register!()
    ZarrCore.compressortypes["zstd"] = ZstdCompressor
    V3Codecs.register_codec("zstd", ZstdV3Codec) do config, ctx
        ZstdV3Codec(get(config, "level", 3))
    end
end

@static if VERSION >= v"1.11"
    include("public_names_zstd.jl")
end

end # module
