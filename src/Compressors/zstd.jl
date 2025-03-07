#=
# Zstd compression

This file implements a Zstd compressor via ChunkCodecLibZstd.jl.

=#

using ChunkCodecLibZstd: ZstdEncodeOptions, encode, decode, ChunkCodecCore


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

function zuncompress(a, ::ZstdCompressor, T)
    result = decode(z.config.codec, a)
    _reinterpret(Base.nonmissingtype(T),result)
end

function zuncompress!(data::DenseArray, compressed, z::ZstdCompressor)
    dst = reinterpret(UInt8, vec(data))
    n = length(dst)
    n_decoded = something(ChunkCodecCore.try_decode!(z.config.codec, dst, compressed))::Int64
    n_decoded == n || error("expected to decode $n bytes, only got $n_decoded bytes")
    data
end

function zcompress(a, z::ZstdCompressor)
    encode(z.config, reinterpret(UInt8, vec(a)))
end

JSON.lower(z::ZstdCompressor) = Dict("id"=>"zstd", "level" => z.config.compressionLevel, "checksum" => z.config.checksum)

Zarr.compressortypes["zstd"] = ZstdCompressor