module V3Codecs

import ..Codecs: zencode, zdecode, zencode!, zdecode!
# Import compressor types and functions from Zarr (grandparent module)
import ...Zarr: ZlibCompressor, ZstdCompressor, zcompress, zuncompress
import ...Zarr: BloscCompressor as ZarrBloscCompressor
import ...Zarr: AbstractCodecPipeline, V3Pipeline, pipeline_encode, pipeline_decode!
using CRC32c: CRC32c
using JSON: JSON
using ChunkCodecLibZlib: GzipCodec as LibZGzipCodec, GzipEncodeOptions
using ChunkCodecCore: encode as cc_encode, decode as cc_decode

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
    endian::Symbol  # :little or :big
    function BytesCodec(endian::Symbol)
        endian ∈ (:little, :big) ||
            throw(ArgumentError("BytesCodec endian must be :little or :big, got :$endian"))
        new(endian)
    end
end
BytesCodec() = BytesCodec(:little)
name(::BytesCodec) = "bytes"

const _SYSTEM_LITTLE_ENDIAN = Base.ENDIAN_BOM == 0x04030201
_needs_bswap(endian::Symbol) = (endian == :little) != _SYSTEM_LITTLE_ENDIAN

struct CRC32cCodec <: V3Codec{:bytes, :bytes}
end
name(::CRC32cCodec) = "crc32c"

struct GzipCodec <: V3Codec{:bytes, :bytes}
end
name(::GzipCodec) = "gzip"

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

"""
    ShardingCodec{N,P1,P2}

Sharding codec for Zarr v3. Splits an outer chunk (shard) into inner chunks, stores
them concatenated with an index that maps inner chunk coordinates to byte ranges.

# Fields
- `chunk_shape`: Shape of each inner chunk (Julia column-major order)
- `codecs`: `V3Pipeline` for encoding/decoding inner chunk data
- `index_codecs`: `V3Pipeline` for encoding/decoding the shard index
- `index_location`: Location of index in shard file, either `:start` or `:end`
"""
struct ShardingCodec{N, P1<:AbstractCodecPipeline, P2<:AbstractCodecPipeline} <: V3Codec{:array, :bytes}
    chunk_shape::NTuple{N,Int}
    codecs::P1
    index_codecs::P2
    index_location::Symbol
end
name(::ShardingCodec) = "sharding_indexed"

"""
Build a `V3Pipeline` from a flat ordered list of `V3Codec` values.
The list must contain: zero or more array→array codecs, exactly one array→bytes codec,
then zero or more bytes→bytes codecs.
"""
function _codecs_to_v3pipeline(codecs::Vector)
    aa = []
    ab = nothing
    bb = []
    for codec in codecs
        if ab === nothing
            if codec isa V3Codec{:array, :array}
                push!(aa, codec)
            elseif codec isa V3Codec{:array, :bytes}
                ab = codec
            else
                throw(ArgumentError("bytes→bytes codec before array→bytes codec in inner codec chain"))
            end
        else
            push!(bb, codec)
        end
    end
    isnothing(ab) && throw(ArgumentError("No array→bytes codec found in inner codec chain"))
    return V3Pipeline(Tuple(aa), ab, Tuple(bb))
end

"""Flatten a `V3Pipeline` back to an ordered list of codec JSON dicts."""
function _pipeline_to_codec_list(p::V3Pipeline)
    result = Dict[]
    for codec in p.array_array
        push!(result, JSON.lower(codec))
    end
    push!(result, JSON.lower(p.array_bytes))
    for codec in p.bytes_bytes
        push!(result, JSON.lower(codec))
    end
    return result
end

function JSON.lower(c::BytesCodec)
    Dict("name" => "bytes", "configuration" => Dict("endian" => string(c.endian)))
end

"""
    JSON.lower(c::ShardingCodec)

Serialize ShardingCodec to JSON. `chunk_shape` is reversed from Julia column-major
back to C-order as required by the Zarr v3 spec.
"""
function JSON.lower(c::ShardingCodec)
    return Dict(
        "name" => "sharding_indexed",
        "configuration" => Dict(
            "chunk_shape"   => collect(reverse(c.chunk_shape)),
            "codecs"        => _pipeline_to_codec_list(c.codecs),
            "index_codecs"  => _pipeline_to_codec_list(c.index_codecs),
            "index_location" => string(c.index_location)
        )
    )
end

"""
    getCodec(d::Dict)

Deserialize a V3 codec from a JSON dict by dispatching on `d["name"]`.
Used to parse inner codecs of `ShardingCodec`.
"""
function getCodec(d::Dict)
    codec_name = d["name"]
    config = get(d, "configuration", Dict{String,Any}())
    if codec_name == "bytes"
        endian_str = get(config, "endian", "little")
        endian = endian_str == "little" ? :little :
                 endian_str == "big"    ? :big    :
                 throw(ArgumentError("Unknown endian: \"$endian_str\""))
        return BytesCodec(endian)
    elseif codec_name == "transpose"
        perm = Tuple(Int.(config["order"]) .+ 1)
        return TransposeCodec(perm)
    elseif codec_name == "gzip"
        level = get(config, "level", 6)
        return GzipV3Codec(level)
    elseif codec_name == "blosc"
        cname = get(config, "cname", "lz4")
        clevel = get(config, "clevel", 5)
        shuffle_val = get(config, "shuffle", "noshuffle")
        shuffle_int = shuffle_val isa Integer ? shuffle_val :
                      shuffle_val == "noshuffle"  ? 0 :
                      shuffle_val == "shuffle"     ? 1 :
                      shuffle_val == "bitshuffle"  ? 2 :
                      throw(ArgumentError("Unknown shuffle: \"$shuffle_val\"."))
        blocksize = get(config, "blocksize", 0)
        typesize  = get(config, "typesize", 4)
        return BloscV3Codec(string(cname), clevel, shuffle_int, blocksize, typesize)
    elseif codec_name == "zstd"
        level = get(config, "level", 3)
        return ZstdV3Codec(level)
    elseif codec_name == "crc32c"
        return CRC32cV3Codec()
    elseif codec_name == "sharding_indexed"
        return getCodec(ShardingCodec, d)
    else
        throw(ArgumentError("Unsupported inner codec: $codec_name"))
    end
end

"""
    getCodec(::Type{ShardingCodec}, d::Dict)

Deserialize `ShardingCodec` from a JSON configuration dict.
`chunk_shape` is reversed from C-order (Zarr spec) to Julia column-major order.
"""
function getCodec(::Type{ShardingCodec}, d::Dict)
    config = d["configuration"]
    N = length(config["chunk_shape"])
    # Zarr spec stores chunk_shape in C-order (row-major); reverse for Julia column-major
    chunk_shape    = NTuple{N,Int}(reverse(Int.(config["chunk_shape"])))
    data_pipeline  = _codecs_to_v3pipeline([getCodec(cd) for cd in config["codecs"]])
    index_pipeline = _codecs_to_v3pipeline([getCodec(cd) for cd in config["index_codecs"]])
    index_location = Symbol(get(config, "index_location", "end"))
    return ShardingCodec(chunk_shape, data_pipeline, index_pipeline, index_location)
end

const MAX_UINT64 = typemax(UInt64)

"""Information about a single inner chunk's location within a shard."""
struct ChunkShardInfo
    offset::UInt64
    nbytes::UInt64
end
ChunkShardInfo() = ChunkShardInfo(MAX_UINT64, MAX_UINT64)  # sentinel: empty chunk

"""N-dimensional array of `ChunkShardInfo`, one entry per inner chunk in a shard."""
struct ShardIndex{N}
    chunks::Array{ChunkShardInfo, N}
end

function ShardIndex(chunks_per_shard::NTuple{N,Int}) where N
    return ShardIndex{N}(fill(ChunkShardInfo(), chunks_per_shard))
end

function get_chunk_slice(idx::ShardIndex, chunk_coords::NTuple{N,Int}) where N
    info = idx.chunks[chunk_coords...]
    info.offset == MAX_UINT64 && info.nbytes == MAX_UINT64 && return nothing
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

"""Return the Julia array slice ranges for inner chunk `chunk_coords` within a shard."""
function get_chunk_slice_in_shard(chunk_coords::NTuple{N,Int}, chunk_shape::NTuple{N,Int}, shard_shape::NTuple{N,Int}) where N
    return ntuple(N) do i
        start_idx = (chunk_coords[i] - 1) * chunk_shape[i] + 1
        end_idx   = min(chunk_coords[i] * chunk_shape[i], shard_shape[i])
        start_idx:end_idx
    end
end

"""
Encode the shard index using the codec's index pipeline.
The index is linearized in column-major order (matching `CartesianIndices` iteration)
with alternating offset/nbytes values per inner chunk.
"""
function encode_shard_index(index::ShardIndex{N}, c::ShardingCodec) where N
    n_chunks   = length(index.chunks)
    index_data = Vector{UInt64}(undef, 2 * n_chunks)
    idx = 1
    for cart_idx in CartesianIndices(index.chunks)
        info = index.chunks[cart_idx]
        index_data[idx]     = info.offset
        index_data[idx + 1] = info.nbytes
        idx += 2
    end
    return pipeline_encode(c.index_codecs, index_data, nothing)
end

"""Decode the shard index from bytes using the codec's index pipeline."""
function decode_shard_index(index_bytes::Vector{UInt8}, chunks_per_shard::NTuple{N,Int}, c::ShardingCodec) where N
    n_chunks   = prod(chunks_per_shard)
    index_data = Vector{UInt64}(undef, n_chunks * 2)
    pipeline_decode!(c.index_codecs, index_data, index_bytes)

    chunks = Array{ChunkShardInfo, N}(undef, chunks_per_shard)
    idx = 1
    for cart_idx in CartesianIndices(chunks)
        chunks[cart_idx] = ChunkShardInfo(index_data[idx], index_data[idx + 1])
        idx += 2
    end
    return ShardIndex{N}(chunks)
end

"""Compute the encoded byte size of the shard index by encoding an empty index."""
function compute_encoded_index_size(chunks_per_shard::NTuple{N,Int}, c::ShardingCodec) where N
    return length(encode_shard_index(ShardIndex(chunks_per_shard), c))
end

"""
    zencode!(encoded, data, c::ShardingCodec)

Encode `data` (a full outer chunk / shard) into sharded binary format.
Inner chunks are encoded with `c.codecs`; the index is encoded with `c.index_codecs`.
"""
function zencode!(encoded::Vector{UInt8}, data::AbstractArray, c::ShardingCodec{N}) where N
    shard_shape      = size(data)
    chunks_per_shard = calculate_chunks_per_shard(shard_shape, c.chunk_shape)

    index          = ShardIndex(chunks_per_shard)
    chunk_buffers  = Vector{UInt8}[]
    current_offset = 0

    for cart_idx in CartesianIndices(chunks_per_shard)
        chunk_coords  = Tuple(cart_idx)
        slice_ranges  = get_chunk_slice_in_shard(chunk_coords, c.chunk_shape, shard_shape)
        encoded_chunk = pipeline_encode(c.codecs, data[slice_ranges...], nothing)

        if isnothing(encoded_chunk) || isempty(encoded_chunk)
            set_chunk_empty!(index, chunk_coords)
            continue
        end

        nbytes = length(encoded_chunk)
        set_chunk_slice!(index, chunk_coords, current_offset, nbytes)
        push!(chunk_buffers, encoded_chunk)
        current_offset += nbytes
    end

    encoded_index = encode_shard_index(index, c)
    index_size    = length(encoded_index)

    # If index is at the start, shift all chunk offsets to account for it
    if c.index_location == :start
        for cart_idx in CartesianIndices(chunks_per_shard)
            chunk_coords = Tuple(cart_idx)
            info = index.chunks[cart_idx]
            if info.offset != MAX_UINT64
                index.chunks[cart_idx] = ChunkShardInfo(info.offset + index_size, info.nbytes)
            end
        end
        encoded_index = encode_shard_index(index, c)
    end

    if isempty(chunk_buffers)
        resize!(encoded, 0)
        return encoded
    end

    resize!(encoded, index_size + current_offset)
    output = IOBuffer(encoded, write=true)
    if c.index_location == :start
        write(output, encoded_index)
        for buf in chunk_buffers; write(output, buf); end
    else
        for buf in chunk_buffers; write(output, buf); end
        write(output, encoded_index)
    end

    return encoded
end

"""
    zdecode!(data, encoded, c::ShardingCodec)

Decode sharded bytes into `data` (a full outer chunk / shard).
"""
function zdecode!(data::AbstractArray, encoded::Vector{UInt8}, c::ShardingCodec{N}) where N
    if isempty(encoded)
        fill!(data, zero(eltype(data)))
        return data
    end

    T                = eltype(data)
    shard_shape      = size(data)
    chunks_per_shard = calculate_chunks_per_shard(shard_shape, c.chunk_shape)
    index_size       = compute_encoded_index_size(chunks_per_shard, c)

    if c.index_location == :start
        index_bytes       = encoded[1:index_size]
        chunk_data_offset = index_size
    else
        index_bytes       = encoded[end-index_size+1:end]
        chunk_data_offset = 0
    end

    index = decode_shard_index(index_bytes, chunks_per_shard, c)

    for cart_idx in CartesianIndices(chunks_per_shard)
        chunk_coords = Tuple(cart_idx)
        array_slice  = get_chunk_slice_in_shard(chunk_coords, c.chunk_shape, shard_shape)
        chunk_slice  = get_chunk_slice(index, chunk_coords)

        if chunk_slice === nothing
            data[array_slice...] .= zero(T)
            continue
        end

        offset_start, offset_end = chunk_slice
        encoded_chunk = encoded[chunk_data_offset + offset_start + 1 : chunk_data_offset + offset_end]

        # Decode into a full inner chunk buffer, then copy (slicing if partial at shard edge)
        output_chunk = Array{T}(undef, c.chunk_shape)
        pipeline_decode!(c.codecs, output_chunk, encoded_chunk)

        inner_shape = ntuple(i -> length(array_slice[i]), N)
        if inner_shape == c.chunk_shape
            data[array_slice...] = output_chunk
        else
            data[array_slice...] = output_chunk[ntuple(i -> 1:inner_shape[i], N)...]
        end
    end

    return data
end

struct TransposeCodec{N} <: V3Codec{:array, :array}
    order::NTuple{N, Int}  # permutation (1-based Julia indexing)
end
name(::TransposeCodec) = "transpose"

function JSON.lower(c::TransposeCodec)
    Dict("name" => "transpose", "configuration" => Dict("order" => collect(c.order .- 1)))
end

# codec_encode / codec_decode methods for V3 codecs

function codec_encode(c::BytesCodec, data::AbstractArray)
    if _needs_bswap(c.endian)
        return reinterpret(UInt8, bswap.(vec(data))) |> collect
    else
        return reinterpret(UInt8, vec(data)) |> collect
    end
end

function codec_decode(c::BytesCodec, encoded::Vector{UInt8}, ::Type{T}, shape::NTuple{N,Int}) where {T, N}
    arr = collect(reinterpret(T, encoded))
    if _needs_bswap(c.endian)
        arr = bswap.(arr)
    end
    return reshape(arr, shape)
end

function codec_encode(c::ShardingCodec, data::AbstractArray)
    encoded = UInt8[]
    zencode!(encoded, data, c)
    return encoded
end

function codec_decode(c::ShardingCodec, encoded::Vector{UInt8}, ::Type{T}, shape::NTuple{N,Int}) where {T, N}
    output = Array{T, N}(undef, shape)
    zdecode!(output, encoded, c)
    return output
end

"""Return the shape of the output of `codec_encode(codec, data)` given the input shape."""
encoded_shape(::V3Codec, sz::NTuple{N,Int}) where {N} = sz
encoded_shape(c::TransposeCodec, sz::NTuple{N,Int}) where {N} = ntuple(i -> sz[c.order[i]], Val{N}())

function codec_encode(c::TransposeCodec, data::AbstractArray)
    return permutedims(data, c.order)
end

function codec_decode(c::TransposeCodec, encoded::AbstractArray)
    inv_order = Tuple(invperm(collect(c.order)))
    return permutedims(encoded, inv_order)
end

struct GzipV3Codec <: V3Codec{:bytes, :bytes}
    level::Int
end
GzipV3Codec() = GzipV3Codec(6)
name(::GzipV3Codec) = "gzip"

function JSON.lower(c::GzipV3Codec)
    Dict("name" => "gzip", "configuration" => Dict("level" => c.level))
end

function codec_encode(c::GzipV3Codec, data::Vector{UInt8})
    opts = GzipEncodeOptions(; level=c.level)
    return cc_encode(opts, data)
end

function codec_decode(c::GzipV3Codec, encoded::Vector{UInt8})
    return cc_decode(LibZGzipCodec(), encoded)
end

struct BloscV3Codec <: V3Codec{:bytes, :bytes}
    cname::String
    clevel::Int
    shuffle::Int
    blocksize::Int
    typesize::Int
end
BloscV3Codec() = BloscV3Codec("lz4", 5, 1, 0, 4)
name(::BloscV3Codec) = "blosc"

function JSON.lower(c::BloscV3Codec)
    shuffle_str = c.shuffle == 0 ? "noshuffle" :
                  c.shuffle == 1 ? "shuffle" :
                  c.shuffle == 2 ? "bitshuffle" :
                  throw(ArgumentError("Unknown shuffle integer: $(c.shuffle)"))
    Dict("name" => "blosc", "configuration" => Dict(
        "cname"     => c.cname,
        "clevel"    => c.clevel,
        "shuffle"   => shuffle_str,
        "blocksize" => c.blocksize,
        "typesize"  => c.typesize
    ))
end

function codec_encode(c::BloscV3Codec, data::Vector{UInt8})
    comp = ZarrBloscCompressor(blocksize=c.blocksize, clevel=c.clevel, cname=c.cname, shuffle=c.shuffle)
    return zcompress(data, comp)
end

function codec_decode(c::BloscV3Codec, encoded::Vector{UInt8})
    comp = ZarrBloscCompressor(blocksize=c.blocksize, clevel=c.clevel, cname=c.cname, shuffle=c.shuffle)
    return collect(zuncompress(encoded, comp, UInt8))
end

struct ZstdV3Codec <: V3Codec{:bytes, :bytes}
    level::Int
end
ZstdV3Codec() = ZstdV3Codec(3)
name(::ZstdV3Codec) = "zstd"

function JSON.lower(c::ZstdV3Codec)
    Dict("name" => "zstd", "configuration" => Dict("level" => c.level))
end

function codec_encode(c::ZstdV3Codec, data::Vector{UInt8})
    comp = ZstdCompressor(level=c.level)
    return zcompress(data, comp)
end

function codec_decode(c::ZstdV3Codec, encoded::Vector{UInt8})
    comp = ZstdCompressor(level=c.level)
    return collect(zuncompress(encoded, comp, UInt8))
end

struct CRC32cV3Codec <: V3Codec{:bytes, :bytes}
end
name(::CRC32cV3Codec) = "crc32c"

function JSON.lower(::CRC32cV3Codec)
    Dict("name" => "crc32c")
end

function codec_encode(c::CRC32cV3Codec, data::Vector{UInt8})
    out = UInt8[]
    return zencode!(out, data, CRC32cCodec())
end

function codec_decode(c::CRC32cV3Codec, encoded::Vector{UInt8})
    out = UInt8[]
    return zdecode!(out, encoded, CRC32cCodec())
end

end
