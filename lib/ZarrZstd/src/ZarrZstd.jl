"""
    ZarrZstd

Zstandard support for Zarr.jl: the zarr v2 [`ZstdCompressor`](@ref) and the
matching zarr v3 [`ZstdV3Codec`](@ref), both backed by ChunkCodecLibZstd.jl.

This is a subpackage of Zarr.jl; its public API is re-exported by `Zarr`, so
`Zarr.ZstdCompressor` keeps working exactly as before.
"""
module ZarrZstd

import JSON # for JSON.lower

using ChunkCodecLibZstd: ZstdEncodeOptions
using ChunkCodecCore: encode, decode, decode!

# Only the names that are used unqualified live here. Methods that *extend* a
# ZarrCore generic are always written as `ZarrCore.f(...)` (or
# `V3Codecs.f(...)`) below: writing a bare `f(...)` definition would silently
# create a new `ZarrZstd.f` that shadows the generic instead of adding a method
# to it, and nothing in ZarrCore would ever see it.
import ZarrCore
using ZarrCore: Compressor
using ZarrCore.Codecs: V3Codecs
using ZarrCore.Codecs.V3Codecs: V3Codec

# `reinterpret` needs a 1-based, non-zero-dimensional array; a 0-d chunk has to
# be reshaped first. Kept local rather than shared, so that this package depends
# only on ZarrCore's documented API.
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

# Both registries live in `ZarrCore`, so the entries have to be added at *load*
# time, not at precompile time: a mutation of another package's global state
# made while this module's body runs is discarded when the precompiled image is
# written out, and the entry would simply be missing in every fresh session.
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
