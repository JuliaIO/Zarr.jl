# V3 compression codecs that depend on Blosc, ChunkCodecLibZlib, ChunkCodecLibZstd, CRC32c
# These register into ZarrCore's V3 codec registry.

using CRC32c: CRC32c
using ChunkCodecLibZlib: GzipCodec as LibZGzipCodec, GzipEncodeOptions
using ChunkCodecCore: encode as cc_encode, decode as cc_decode

import ZarrCore.Codecs.V3Codecs: V3Codec, v3_codec_parsers, codec_to_dict,
    codec_encode, codec_decode

# --- CRC32cCodec (internal helper) ---

struct CRC32cCodec
end

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

# --- GzipV3Codec ---

struct GzipV3Codec <: V3Codec{:bytes, :bytes}
    level::Int
end
GzipV3Codec() = GzipV3Codec(6)

function codec_encode(c::GzipV3Codec, data::Vector{UInt8})
    opts = GzipEncodeOptions(; level=c.level)
    return cc_encode(opts, data)
end
function codec_decode(c::GzipV3Codec, encoded::Vector{UInt8})
    return cc_decode(LibZGzipCodec(), encoded)
end
codec_to_dict(c::GzipV3Codec) = Dict{String,Any}(
    "name" => "gzip",
    "configuration" => Dict{String,Any}("level" => c.level)
)

# Parser registration moved to _register_v3_codec_parsers!()

# --- BloscV3Codec ---

struct BloscV3Codec <: V3Codec{:bytes, :bytes}
    cname::String
    clevel::Int
    shuffle::Int
    blocksize::Int
    typesize::Int
end
BloscV3Codec() = BloscV3Codec("lz4", 5, 1, 0, 4)

function codec_encode(c::BloscV3Codec, data::Vector{UInt8})
    comp = BloscCompressor(blocksize=c.blocksize, clevel=c.clevel, cname=c.cname, shuffle=c.shuffle)
    return zcompress(data, comp)
end
function codec_decode(c::BloscV3Codec, encoded::Vector{UInt8})
    comp = BloscCompressor(blocksize=c.blocksize, clevel=c.clevel, cname=c.cname, shuffle=c.shuffle)
    return collect(zuncompress(encoded, comp, UInt8))
end
codec_to_dict(c::BloscV3Codec) = Dict{String,Any}(
    "name" => "blosc",
    "configuration" => Dict{String,Any}(
        "cname" => c.cname,
        "clevel" => c.clevel,
        "shuffle" => c.shuffle == 0 ? "noshuffle" :
                     c.shuffle == 1 ? "shuffle" :
                     c.shuffle == 2 ? "bitshuffle" :
                     throw(ArgumentError("Unknown shuffle integer: $(c.shuffle)")),
        "blocksize" => c.blocksize,
        "typesize" => c.typesize
    )
)

# Parser registration moved to _register_v3_codec_parsers!()

# --- ZstdV3Codec ---

struct ZstdV3Codec <: V3Codec{:bytes, :bytes}
    level::Int
end
ZstdV3Codec() = ZstdV3Codec(3)

function codec_encode(c::ZstdV3Codec, data::Vector{UInt8})
    comp = ZstdCompressor(level=c.level)
    return zcompress(data, comp)
end
function codec_decode(c::ZstdV3Codec, encoded::Vector{UInt8})
    comp = ZstdCompressor(level=c.level)
    return collect(zuncompress(encoded, comp, UInt8))
end
codec_to_dict(c::ZstdV3Codec) = Dict{String,Any}(
    "name" => "zstd",
    "configuration" => Dict{String,Any}("level" => c.level)
)

# Parser registration moved to _register_v3_codec_parsers!()

# --- CRC32cV3Codec ---

struct CRC32cV3Codec <: V3Codec{:bytes, :bytes}
end

function codec_encode(c::CRC32cV3Codec, data::Vector{UInt8})
    out = UInt8[]
    return zencode!(out, data, CRC32cCodec())
end
function codec_decode(c::CRC32cV3Codec, encoded::Vector{UInt8})
    out = UInt8[]
    return zdecode!(out, encoded, CRC32cCodec())
end
codec_to_dict(::CRC32cV3Codec) = Dict{String,Any}("name" => "crc32c")

# Parser registration moved to _register_v3_codec_parsers!()

# --- ShardingCodec ---

"""
    ShardingCodec{N}

Sharding codec for Zarr v3. Sharding splits chunks into smaller "shards" and stores them
in a single file with an index mapping chunk coordinates to shard locations.

# Fields
- `chunk_shape`: Shape of each shard (NTuple{N,Int})
- `codecs`: Vector of codecs to apply to shard data (e.g., [BytesCodec(), GzipCodec()])
- `index_codecs`: Vector of codecs to apply to the index (e.g., [BytesCodec()])
- `index_location`: Location of index in shard file, either `:start` or `:end`
"""
struct ShardingCodec{N} <: V3Codec{:array, :bytes}
    chunk_shape::NTuple{N,Int}
    codecs::Vector{V3Codec}
    index_codecs::Vector{V3Codec}
    index_location::Symbol
end

const MAX_UINT64 = typemax(UInt64)

"""
    ChunkShardInfo

Information about a chunk's location within a shard.
"""
struct ChunkShardInfo
    offset::UInt64
    nbytes::UInt64
end

ChunkShardInfo() = ChunkShardInfo(MAX_UINT64, MAX_UINT64)

"""
    ShardIndex{N}

Internal structure representing the shard index.
Stores chunk location info for an N-dimensional grid of chunks.
Empty chunks are marked with ChunkShardInfo(MAX_UINT64, MAX_UINT64)
"""
struct ShardIndex{N}
    chunks::Array{ChunkShardInfo, N}
end

function ShardIndex(chunks_per_shard::NTuple{N,Int}) where N
    chunks = fill(ChunkShardInfo(), chunks_per_shard)
    return ShardIndex{N}(chunks)
end

function get_chunk_slice(idx::ShardIndex, chunk_coords::NTuple{N,Int}) where N
    info = idx.chunks[chunk_coords...]
    if info.offset == MAX_UINT64 && info.nbytes == MAX_UINT64
        return nothing
    end
    return (Int(info.offset), Int(info.offset + info.nbytes))
end

function set_chunk_slice!(idx::ShardIndex, chunk_coords::NTuple{N,Int}, offset::Int, nbytes::Int) where N
    idx.chunks[chunk_coords...] = ChunkShardInfo(UInt64(offset), UInt64(nbytes))
end

function set_chunk_empty!(idx::ShardIndex, chunk_coords::NTuple{N,Int}) where N
    idx.chunks[chunk_coords...] = ChunkShardInfo()
end

function calculate_chunks_per_shard(shard_shape::NTuple{N,Int}, chunk_shape::NTuple{N,Int}) where N
    return ntuple(i -> div(shard_shape[i], chunk_shape[i]), N)
end

function get_chunk_slice_in_shard(chunk_coords::NTuple{N,Int}, chunk_shape::NTuple{N,Int}, shard_shape::NTuple{N,Int}) where N
    return ntuple(N) do i
        start_idx = (chunk_coords[i] - 1) * chunk_shape[i] + 1
        end_idx = min(chunk_coords[i] * chunk_shape[i], shard_shape[i])
        start_idx:end_idx
    end
end

function apply_codec_chain(data, codecs::Vector{V3Codec})
    result = data
    for codec in codecs
        result = zencode(result, codec)
    end
    return result
end

function reverse_codec_chain(data, codecs::Vector{V3Codec})
    result = data
    for codec in reverse(codecs)
        result = zdecode(result, codec)
    end
    return result
end

function encode_shard_index(index::ShardIndex{N}, index_codecs::Vector{V3Codec}) where N
    n_chunks = length(index.chunks)
    index_data = Vector{UInt64}(undef, 2 * n_chunks)
    idx = 1
    for cart_idx in CartesianIndices(index.chunks)
        info = index.chunks[cart_idx]
        index_data[idx] = info.offset
        index_data[idx + 1] = info.nbytes
        idx += 2
    end
    index_bytes = reinterpret(UInt8, index_data)
    encoded = apply_codec_chain(index_bytes, index_codecs)
    return encoded
end

function decode_shard_index(index_bytes::Vector{UInt8}, chunks_per_shard::NTuple{N,Int}, index_codecs::Vector{V3Codec}) where N
    decoded_bytes = reverse_codec_chain(index_bytes, index_codecs)
    n_chunks = prod(chunks_per_shard)
    expected_length = n_chunks * 2 * sizeof(UInt64)
    if length(decoded_bytes) != expected_length
        throw(DimensionMismatch("Index size mismatch: expected $expected_length, got $(length(decoded_bytes))"))
    end
    index_data = reinterpret(UInt64, decoded_bytes)
    chunks = Array{ChunkShardInfo, N}(undef, chunks_per_shard)
    idx = 1
    for cart_idx in CartesianIndices(chunks)
        offset = index_data[idx]
        nbytes = index_data[idx + 1]
        chunks[cart_idx] = ChunkShardInfo(offset, nbytes)
        idx += 2
    end
    return ShardIndex{N}(chunks)
end

function compute_encoded_index_size(chunks_per_shard::NTuple{N,Int}, index_codecs::Vector{V3Codec}) where N
    index = ShardIndex(chunks_per_shard)
    encoded = encode_shard_index(index, index_codecs)
    return length(encoded)
end

# Sharding codec parser registration moved to _register_v3_codec_parsers!()

"""
    _register_v3_codec_parsers!()

Register all V3 codec parsers into ZarrCore's registry. Called from Zarr.__init__().
"""
function _register_v3_codec_parsers!()
    v3_codec_parsers["gzip"] = function(config)
        level = get(config, "level", 6)
        GzipV3Codec(level)
    end

    v3_codec_parsers["blosc"] = function(config)
        cname = get(config, "cname", "lz4")
        clevel = get(config, "clevel", 5)
        shuffle_val = get(config, "shuffle", "noshuffle")
        shuffle_int = shuffle_val isa Integer ? shuffle_val :
                      shuffle_val == "noshuffle" ? 0 :
                      shuffle_val == "shuffle" ? 1 :
                      shuffle_val == "bitshuffle" ? 2 :
                      throw(ArgumentError("Unknown shuffle: \"$shuffle_val\"."))
        blocksize = get(config, "blocksize", 0)
        typesize = get(config, "typesize", 4)
        BloscV3Codec(string(cname), clevel, shuffle_int, blocksize, typesize)
    end

    v3_codec_parsers["zstd"] = function(config)
        level = get(config, "level", 3)
        ZstdV3Codec(level)
    end

    v3_codec_parsers["crc32c"] = function(config)
        CRC32cV3Codec()
    end

    v3_codec_parsers["sharding_indexed"] = function(config)
        throw(ArgumentError("Zarr.jl currently does not support the sharding_indexed codec"))
    end
end
