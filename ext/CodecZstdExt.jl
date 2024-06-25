module CodecZstdExt

using Zarr: Zarr
using JSON: JSON
using CodecZstd: CodecZstd

"""
    ZstdZarrCompressor(clevel::Int=0)
    ZstdZarrCompressor(c::CodecZstd.ZstdCompressor, [d::CodecZstd.ZstdDecompressor])

Zstandard compression for Zarr.jl. This is a `Zarr.Compressor` wrapper around
`CodecZstd`. Construct with either the compression level, `clevel`, or by
providing an instance of a `ZstdCompressor`.  `ZstdFrameCompressor` is
recommended.
"""
struct ZstdZarrCompressor <: Zarr.Compressor
    compressor::CodecZstd.ZstdCompressor
    decompressor::CodecZstd.ZstdDecompressor
end
# Use default ZstdDecompressor if only compressor is provided
function ZstdZarrCompressor(compressor::CodecZstd.ZstdCompressor)
    return ZstdZarrCompressor(
        compressor,
        CodecZstd.ZstdDecompressor()
    )
end
function ZstdZarrCompressor(clevel::Int)
    return ZstdZarrCompressor(
        CodecZstd.ZstdFrameCompressor(; level = clevel)
    )
end
ZstdZarrCompressor(;clevel::Int=3) = ZstdZarrCompressor(clevel)

function Zarr.getCompressor(::Type{ZstdZarrCompressor}, d::Dict)
    return ZstdZarrCompressor(d["level"])
end

function Zarr.zuncompress(a, z::ZstdZarrCompressor, T)
    @info "1" a
    result = transcode(z.decompressor, a)
    @info "2" result
    return Zarr._reinterpret(Base.nonmissingtype(T), result)
end

function Zarr.zcompress(a, z::ZstdZarrCompressor)
    a_uint8 = Zarr._reinterpret(UInt8,a)[:]
    transcode(z.compressor, a_uint8)
end

JSON.lower(z::ZstdZarrCompressor) = Dict("id"=>"zstd", "level" => z.compressor.level)

function __init__()
    Zarr.compressortypes["zstd"] = ZstdZarrCompressor
end

end # module CodecZstdExt
