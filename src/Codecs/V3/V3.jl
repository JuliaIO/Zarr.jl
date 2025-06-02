module V3Codecs

import ..Codecs: zencode, zdecode, zencode!, zdecode!
using CRC32c: CRC32c

abstract type V3Codec{In,Out} end
const codectypes = Dict{String, V3Codec}()

@enum BloscCompressor begin
    lz4
    lz4hc
    blosclz
    zstd
    snappy
    zlib
end

@enum BloscShuffle begin
    noshuffle
    shuffle
    bitshuffle
end

struct BloscCodec <: V3Codec{:bytes, :bytes}
    cname::BloscCompressor
    clevel::Int64
    shuffle::BloscShuffle
    typesize::UInt8
    blocksize::UInt
end
name(::BloscCodec) = "blosc"

struct BytesCodec <: V3Codec{:array, :bytes}
end
name(::BytesCodec) = "bytes"

struct CRC32cCodec <: V3Codec{:bytes, :bytes}
end
name(::CRC32cCodec) = "crc32c"

struct GzipCodec <: V3Codec{:bytes, :bytes}
end
name(::GzipCodec) = "gzip"


#=
zencode(a, c::Codec) = error("Unimplemented")
zencode!(encoded, data, c::Codec) = error("Unimplemented")
zdecode(a, c::Codec, T::Type) = error("Unimplemented")
zdecode!(data, encoded, c::Codec) = error("Unimplemented")
=#

function crc32c_stream!(output::IO, input::IO; buffer = Vector{UInt8}(undef, 1024*32))
    hash::UInt32 = 0x00000000
    while(bytesavailable(input) > 0)
        sized_buffer = @view(buffer[1:min(length(buffer), bytesavailable(input))])
        read!(input, sized_buffer)
        write(output, sized_buffer)
        hash = CRC32c.crc32c(sized_buffer, hash)
    end
    return hash
end
function zencode!(encoded::Vector{UInt8}, data::Vector{UInt8}, c::CRC32cCodec)
    output = IOBuffer(encoded, read=false, write=true)
    input = IOBuffer(data, read=true, write=false)
    zencode!(output, input, c)
    return take!(output)
end
function zencode!(output::IO, input::IO, c::CRC32cCodec)
    hash = crc32c_stream!(output, input)
    write(output, hash)
    return output
end
function zdecode!(encoded::Vector{UInt8}, data::Vector{UInt8}, c::CRC32cCodec)
    output = IOBuffer(encoded, read=false, write=true)
    input = IOBuffer(data, read=true, write=true)
    zdecode!(output, input, c)
    return take!(output)
end
function zdecode!(output::IOBuffer, input::IOBuffer, c::CRC32cCodec)
    input_vec = take!(input)
    truncated_input = IOBuffer(@view(input_vec[1:end-4]); read=true, write=false)
    hash = crc32c_stream!(output, truncated_input)
    if input_vec[end-3:end] != reinterpret(UInt8, [hash])
        throw(IOError("CRC32c hash does not match"))
    end
    return output
end

struct ShardingCodec{N} <: V3Codec{:array, :bytes}
    chunk_shape::NTuple{N,Int}
    codecs::Vector{V3Codec}
    index_codecs::Vector{V3Codec}
    index_location::Symbol
end
name(::ShardingCodec) = "sharding_indexed"

struct TransposeCodec <: V3Codec{:array, :array}
end
name(::TransposeCodec) = "transpose"


end
