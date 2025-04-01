# This is a stub for ChunkCodecLibZstdExt.ZstdCompressor()

"""
    ZstdCompressor(;level=0, checksum=false)
Returns a `ZstdCompressor` struct that can serve as a Zarr array compressor. Keyword arguments are:
* `level=0`: the compression level, regular levels are 1 to 22, 0 is a special value for default, there are also even faster negative levels.
* `checksum=false`: flag to enable saving checksums.
"""
function ZstdCompressor(; kwargs...)
    ChunkCodecLibZstdExt = Base.get_extension(Zarr, :ChunkCodecLibZstdExt)
    if isnothing(ChunkCodecLibZstdExt)
        error("Please load ChunkCodecLibZstd.jl by typing `using ChunkCodecLibZstd`")
    else
        return ChunkCodecLibZstdExt.ZstdCompressor(; kwargs...)
    end
end
