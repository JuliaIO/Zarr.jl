"""
    Compressor v3{C <: Compressor} <: Compressor

Wrapper to indicate Zarr v3 of a compressor
"""
struct Compressor_v3{C} <: Compressor
    parent::C
end
Base.parent(c::Compressor_v3) = c.parent

function zuncompress(a, z::Compressor_v3, T)
    zuncompress(a, parent(z), T)
end

function zuncompress!(data::DenseArray, compressed, z::Compressor_v3)
    zuncompress!(data, compressed, parent(z))
end

function zcompress(a, z::Compressor_v3)
    zcompress(a, parent(z))
end


function JSON.lower(c::Compressor_v3{BloscCompressor})
    p = parent(c)
    return Dict(
        "name" => "blosc",
        "configuration" => Dict(
            "cname" => p.cname,
            "clevel" => p.clevel,
            "shuffle" => p.shuffle,
# TODO: Evaluate if we can encode typesize
#            "typesize" => p.typesize,
            "blocksize" => p.blocksize
        )
    )
end

function JSON.lower(c::Compressor_v3{ZlibCompressor})
    p = parent(c)
    return Dict(
        "name" => "gzip",
        "configuration" => Dict(
            "level" => p.config.level
        )
    )
end

function JSON.lower(c::Compressor_v3{ZstdCompressor})
    p = parent(c)
    return Dict(
        "name" => "zstd",
        "configuration" => Dict(
            "level" => p.config.compressionlevel,
            "checksum" => p.config.checksum
        )
    )
end
