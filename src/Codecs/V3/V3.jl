module V3Codecs

import ..Codecs: zencode, zdecode, zencode!, zdecode!
using CRC32c: CRC32c
using JSON: JSON

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

"""
    ShardingCodec{N}

Sharding codec for Zarr v3. Sharding splits chunks into smaller "shards" and stores them
in a single file with an index mapping chunk coordinates to shard locations.

# Fields
- `chunk_shape`: Shape of each shard (NTuple{N,Int})
- `codecs`: Vector of codecs to apply to shard data (e.g., [BytesCodec(), GzipCodec()])
- `index_codecs`: Vector of codecs to apply to the index (e.g., [BytesCodec()])
- `index_location`: Location of index in shard file, either `:start` or `:end`

# Implementation Notes
Sharding works by:
1. Taking a chunk of data and splitting it into shards based on `chunk_shape`
2. Encoding each shard using the `codecs` pipeline
3. Creating an index that maps (chunk_coords, shard_coords) -> (offset, size) in the shard file
4. Encoding the index using `index_codecs`
5. Writing the shard file with index at `index_location` (start or end)

"""
struct ShardingCodec{N} <: V3Codec{:array, :bytes}
    chunk_shape::NTuple{N,Int}  # Shape of each shard
    codecs::Vector{V3Codec}     # Codecs to apply to shard data
    index_codecs::Vector{V3Codec}  # Codecs to apply to the index
    index_location::Symbol      # :start or :end
end
name(::ShardingCodec) = "sharding_indexed"

"""
    JSON.lower(c::ShardingCodec)

Serialize ShardingCodec to JSON format for Zarr v3 metadata.
"""
function JSON.lower(c::ShardingCodec)
    return Dict(
        "name" => "sharding_indexed",
        "configuration" => Dict(
            "chunk_shape" => collect(c.chunk_shape),
            "codecs" => [JSON.lower(codec) for codec in c.codecs],
            "index_codecs" => [JSON.lower(codec) for codec in c.index_codecs],
            "index_location" => string(c.index_location)
        )
    )
end

"""
    getCodec(::Type{ShardingCodec}, d::Dict)

Deserialize ShardingCodec from JSON configuration dict.
"""
function getCodec(::Type{ShardingCodec}, d::Dict)
    config = d["configuration"]
    N = length(config["chunk_shape"])
    chunk_shape = NTuple{N,Int}(config["chunk_shape"])
    codecs = [getCodec(codec_dict) for codec_dict in config["codecs"]]
    index_codecs = [getCodec(codec_dict) for codec_dict in config["index_codecs"]]
    index_location = Symbol(get(config, "index_location", "end"))
    return ShardingCodec{N}(chunk_shape, codecs, index_codecs, index_location)
end

const MAX_UINT64 = typemax(UInt64)

"""
    ShardIndex{N}

Internal structure representing the shard index.
Shape: (chunks_per_shard..., 2) where last dimension is [offset, nbytes]
Empty chunks are marked with (MAX_UINT64, MAX_UINT64)
"""
struct ShardIndex{N}
    offsets_and_lengths::Array{UInt64, N}  # Shape: (chunks_per_shard..., 2)
end

"""
    ShardIndex(chunks_per_shard::NTuple{N,Int})

Create an empty shard index with all chunks marked as empty.
"""
function ShardIndex(chunks_per_shard::NTuple{N,Int}) where N
    arr = fill(MAX_UINT64, (chunks_per_shard..., 2))
    return ShardIndex{N+1}(arr)
end

"""
    get_chunk_slice(idx::ShardIndex, chunk_coords::NTuple{N,Int})

Get the byte range (offset, offset+nbytes) for a chunk, or nothing if empty.
"""
function get_chunk_slice(idx::ShardIndex, chunk_coords::NTuple{N,Int}) where N
    offset = idx.offsets_and_lengths[chunk_coords..., 1]
    nbytes = idx.offsets_and_lengths[chunk_coords..., 2]
    
    if offset == MAX_UINT64 && nbytes == MAX_UINT64
        return nothing
    end
    
    return (Int(offset), Int(offset + nbytes))
end

"""
    set_chunk_slice!(idx::ShardIndex, chunk_coords::NTuple{N,Int}, offset::Int, nbytes::Int)

Set the byte range for a chunk in the index.
"""
function set_chunk_slice!(idx::ShardIndex, chunk_coords::NTuple{N,Int}, offset::Int, nbytes::Int) where N
    idx.offsets_and_lengths[chunk_coords..., 1] = UInt64(offset)
    idx.offsets_and_lengths[chunk_coords..., 2] = UInt64(nbytes)
end

"""
    set_chunk_empty!(idx::ShardIndex, chunk_coords::NTuple{N,Int})

Mark a chunk as empty in the index.
"""
function set_chunk_empty!(idx::ShardIndex, chunk_coords::NTuple{N,Int}) where N
    idx.offsets_and_lengths[chunk_coords..., 1] = MAX_UINT64
    idx.offsets_and_lengths[chunk_coords..., 2] = MAX_UINT64
end

"""
    calculate_chunks_per_shard(shard_shape::NTuple{N,Int}, chunk_shape::NTuple{N,Int})

Calculate how many chunks fit in each shard dimension.
"""
function calculate_chunks_per_shard(shard_shape::NTuple{N,Int}, chunk_shape::NTuple{N,Int}) where N
    return ntuple(i -> div(shard_shape[i], chunk_shape[i]), N)
end

"""
    get_chunk_slice_in_shard(chunk_coords::NTuple{N,Int}, chunk_shape::NTuple{N,Int}, 
                             shard_shape::NTuple{N,Int})

Get the array slice ranges for a chunk within a shard.
chunk_coords are 1-based indices.
"""
function get_chunk_slice_in_shard(chunk_coords::NTuple{N,Int}, chunk_shape::NTuple{N,Int}, 
                                   shard_shape::NTuple{N,Int}) where N
    return ntuple(N) do i
        start_idx = (chunk_coords[i] - 1) * chunk_shape[i] + 1
        end_idx = min(chunk_coords[i] * chunk_shape[i], shard_shape[i])
        start_idx:end_idx
    end
end

"""
    apply_codec_chain(data, codecs::Vector{V3Codec})

Apply codec pipeline in forward order (encoding).
"""
function apply_codec_chain(data, codecs::Vector{V3Codec})
    result = data
    for codec in codecs
        result = zencode(result, codec)
    end
    return result
end

"""
    reverse_codec_chain(data, codecs::Vector{V3Codec})

Apply codec pipeline in reverse order (decoding).
"""
function reverse_codec_chain(data, codecs::Vector{V3Codec})
    result = data
    for codec in reverse(codecs)
        result = zdecode(result, codec)
    end
    return result
end

"""
    encode_shard_index(index::ShardIndex, index_codecs::Vector{V3Codec})

Encode the shard index using the index codec pipeline.
Per spec: "The index is encoded into binary representations using the specified index codecs."
"""
function encode_shard_index(index::ShardIndex{N}, index_codecs::Vector{V3Codec}) where N
    # Index array is stored in C order (row-major)
    # Convert to bytes: the index is an array of UInt64 values
    index_bytes = reinterpret(UInt8, vec(index.offsets_and_lengths))
    
    # Apply index codecs
    encoded = apply_codec_chain(index_bytes, index_codecs)
    
    return encoded
end

"""
    decode_shard_index(index_bytes::Vector{UInt8}, chunks_per_shard::NTuple{N,Int},
                       index_codecs::Vector{V3Codec})

Decode the shard index from bytes.
"""
function decode_shard_index(index_bytes::Vector{UInt8}, chunks_per_shard::NTuple{N,Int},
                            index_codecs::Vector{V3Codec}) where N
    # Decode using index codecs (in reverse order)
    decoded_bytes = reverse_codec_chain(index_bytes, index_codecs)
    
    # Expected size: 16 bytes (2 * UInt64) per chunk
    n_chunks = prod(chunks_per_shard)
    expected_length = n_chunks * 2 * sizeof(UInt64)
    
    if length(decoded_bytes) != expected_length
        throw(DimensionMismatch("Index size mismatch: expected $expected_length, got $(length(decoded_bytes))"))
    end
    
    # Reshape to index array: (chunks_per_shard..., 2)
    index_array = reshape(reinterpret(UInt64, decoded_bytes), (chunks_per_shard..., 2))
    
    return ShardIndex{N+1}(index_array)
end

"""
    compute_encoded_index_size(chunks_per_shard::NTuple{N,Int}, index_codecs::Vector{V3Codec})

Compute the byte size of the encoded shard index.
Per spec: "The size of the index can be determined by applying c.compute_encoded_size 
for each index codec recursively. The initial size is the byte size of the index array, 
i.e. 16 * chunks per shard."
"""
function compute_encoded_index_size(chunks_per_shard::NTuple{N,Int}, index_codecs::Vector{V3Codec}) where N
    # Initial size: 16 bytes per chunk (2 * UInt64)
    n_chunks = prod(chunks_per_shard)
    size = n_chunks * 16
    
    # Apply each codec's size transformation
    # For most codecs, we need to actually encode to know the size
    # For simplicity, we encode an empty index
    index = ShardIndex(chunks_per_shard)
    encoded = encode_shard_index(index, index_codecs)
    
    return length(encoded)
end

"""
    zencode!(encoded::Vector{UInt8}, data::AbstractArray, c::ShardingCodec)

Encode array data using sharding codec following Zarr v3 spec.

Per spec: "In the sharding_indexed binary format, inner chunks are written successively 
in a shard, where unused space between them is allowed, followed by an index referencing them."
"""
function zencode!(encoded::Vector{UInt8}, data::AbstractArray, c::ShardingCodec{N}) where N
    shard_shape = size(data)
    chunks_per_shard = calculate_chunks_per_shard(shard_shape, c.chunk_shape)
    
    # Create empty index
    index = ShardIndex(chunks_per_shard)
    
    # Buffers for encoded chunks
    chunk_buffers = Vector{UInt8}[]
    current_offset = 0
    
    # Process chunks in C order (row-major)
    # Per spec: "The actual order of the chunk content is not fixed"
    for cart_idx in CartesianIndices(chunks_per_shard)
        chunk_coords = Tuple(cart_idx)
        
        # Extract chunk data from shard
        slice_ranges = get_chunk_slice_in_shard(chunk_coords, c.chunk_shape, shard_shape)
        chunk_data = data[slice_ranges...]
        
        # Encode chunk using codec pipeline
        encoded_chunk = apply_codec_chain(chunk_data, c.codecs)
        
        # Skip if chunk is empty (no bytes)
        if isempty(encoded_chunk)
            set_chunk_empty!(index, chunk_coords)
            continue
        end
        
        nbytes = length(encoded_chunk)
        
        # Record offset and length in index
        set_chunk_slice!(index, chunk_coords, current_offset, nbytes)
        
        push!(chunk_buffers, encoded_chunk)
        current_offset += nbytes
    end
    
    # Encode the index
    encoded_index = encode_shard_index(index, c.index_codecs)
    index_size = length(encoded_index)
    
    # If index is at start, adjust all offsets to account for index size
    if c.index_location == :start
        # Add index_size to all non-empty chunk offsets
        for cart_idx in CartesianIndices(chunks_per_shard)
            chunk_coords = Tuple(cart_idx)
            offset = index.offsets_and_lengths[chunk_coords..., 1]
            if offset != MAX_UINT64
                index.offsets_and_lengths[chunk_coords..., 1] = offset + index_size
            end
        end
        # Re-encode index with corrected offsets
        encoded_index = encode_shard_index(index, c.index_codecs)
    end
    
    # If all chunks are empty, return empty buffer (no shard)
    if isempty(chunk_buffers)
        resize!(encoded, 0)
        return encoded
    end
    
    # Assemble final shard: [index] + chunks or chunks + [index]
    total_size = (c.index_location == :start ? index_size : 0) + 
                 current_offset + 
                 (c.index_location == :end ? index_size : 0)
    
    resize!(encoded, total_size)
    output = IOBuffer(encoded, write=true)
    
    if c.index_location == :start
        write(output, encoded_index)
        for buf in chunk_buffers
            write(output, buf)
        end
    else  # :end
        for buf in chunk_buffers
            write(output, buf)
        end
        write(output, encoded_index)
    end
    
    return encoded
end

"""
    zdecode!(data::AbstractArray, encoded::Vector{UInt8}, c::ShardingCodec)

Decode sharded data back to array following Zarr v3 spec.

Per spec: "A simple implementation to decode inner chunks in a shard would 
(a) read the entire value from the store into a byte buffer, 
(b) parse the shard index from the beginning or end of the buffer and 
(c) cut out the relevant bytes that belong to the requested chunk."
"""
function zdecode!(data::AbstractArray, encoded::Vector{UInt8}, c::ShardingCodec{N}) where N
    # Handle empty shard (no data)
    if isempty(encoded)
        fill!(data, zero(eltype(data)))  # Fill with zeros (or should use fill_value from spec)
        return data
    end
    
    shard_shape = size(data)
    chunks_per_shard = calculate_chunks_per_shard(shard_shape, c.chunk_shape)
    
    # Compute encoded index size
    index_size = compute_encoded_index_size(chunks_per_shard, c.index_codecs)
    
    # Extract index bytes based on location
    if c.index_location == :start
        index_bytes = encoded[1:index_size]
        chunk_data_offset = index_size
    else  # :end
        index_bytes = encoded[end-index_size+1:end]
        chunk_data_offset = 0
    end
    
    # Decode the index
    index = decode_shard_index(index_bytes, chunks_per_shard, c.index_codecs)
    
    # Decode each chunk and place into output array
    for cart_idx in CartesianIndices(chunks_per_shard)
        chunk_coords = Tuple(cart_idx)
        
        # Get chunk byte range from index
        chunk_slice = get_chunk_slice(index, chunk_coords)
        
        # Get array slice for this chunk
        array_slice = get_chunk_slice_in_shard(chunk_coords, c.chunk_shape, shard_shape)
        
        if chunk_slice === nothing
            # Empty chunk - fill with zeros (or fill_value)
            # Per spec: "Empty inner chunks are interpreted as being filled with the fill value"
            data[array_slice...] .= zero(eltype(data))
            continue
        end
        
        # Extract chunk bytes
        # Offsets in index are relative to start of chunk data
        offset_start, offset_end = chunk_slice
        
        # Adjust for where chunk data begins in the shard
        byte_start = chunk_data_offset + offset_start + 1  # Julia 1-based indexing
        byte_end = chunk_data_offset + offset_end
        
        encoded_chunk = encoded[byte_start:byte_end]
        
        # Decode chunk using codec pipeline (in reverse)
        decoded_chunk = reverse_codec_chain(encoded_chunk, c.codecs)
        
        # Place decoded chunk into output array
        expected_shape = length.(array_slice)
        data[array_slice...] = reshape(decoded_chunk, expected_shape)
    end
    
    return data
end

struct TransposeCodec <: V3Codec{:array, :array}
end
name(::TransposeCodec) = "transpose"


end
