#=
# Zstd compression

This file implements a Zstd compressor via ChunkCodecLibZstd.jl.

=#
using ChunkCodecLibZstd: ZstdEncodeOptions
using ChunkCodecCore: encode, decode, decode!

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

function getCompressor(::Type{ZstdCompressor}, d::Dict)
    ZstdCompressor(;
        level=get(Returns(0), d, "level"),
        checksum=Bool(get(Returns(false), d, "checksum")),
    )
end

function zuncompress(a, z::ZstdCompressor, T)
    result = decode(z.config.codec, a)
    _reinterpret(Base.nonmissingtype(T),result)
end

function zuncompress!(data::DenseArray, compressed, z::ZstdCompressor)
    decode!(z.config.codec, reinterpret(UInt8, vec(data)), compressed)
    data
end

function zcompress(a, z::ZstdCompressor)
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

Zarr.compressortypes["zstd"] = ZstdCompressor