module V3Codecs

import ..Codecs: zencode, zdecode, zencode!, zdecode!
# Import compressor types and functions from Zarr (grandparent module)
import ...Zarr: ZlibCompressor, ZstdCompressor, zcompress, zuncompress
import ...Zarr: BloscCompressor as ZarrBloscCompressor
import ...Zarr: AbstractCodecPipeline, V3Pipeline, pipeline_encode, pipeline_decode!
import ...Zarr: enable_threaded_shard_decode
using CRC32c: CRC32c
using JSON: JSON
using ChunkCodecLibZlib: GzipCodec as LibZGzipCodec, GzipEncodeOptions
using ChunkCodecCore: encode as cc_encode, decode as cc_decode

abstract type V3Codec{In,Out} end

"""
    is_fixed_size(codec::V3Codec) -> Bool

Return `true` if `codec` always produces output whose byte size is fully
determined by the input structure (shape and element type), never by the
input values.  In other words, it performs no variable-length compression.

Fixed-size codecs may be used in the `index_codecs` pipeline of a
`sharding_indexed` codec.  The default is `false`; codecs opt in by
defining a method that returns `true`.
"""
is_fixed_size(::V3Codec) = false

"""Stores a registered V3 codec parser together with its expected return type."""
struct CodecEntry
    return_type::Type{<:V3Codec}
    parser::Function
end

"""
Registry mapping codec names to `CodecEntry` values (return type + parser function).

Use `register_codec` to add new entries.
"""
const codec_parsers = Dict{String, CodecEntry}()

"""
    register_codec(parser::Function, name::String[, ::Type{T}])

Register a codec parser under `name`. The parser must accept a
`Dict{String,Any}` configuration and a context value (or `nothing`),
and return a `V3Codec`.

The optional trailing `Type{T}` argument narrows the declared return type stored
in the registry (defaults to `V3Codec`). Specifying it enables a runtime
assertion in `getCodec` and makes the registry self-documenting.

Supports do-block syntax:

    register_codec("mycodec") do config, ctx
        MyCodec(config["param"])
    end

    register_codec("mycodec", MyCodec) do config, ctx
        MyCodec(config["param"])
    end
"""
function register_codec(parser::Function, name::String, ::Type{T}) where {T<:V3Codec}
    codec_parsers[name] = CodecEntry(T, parser)
end
register_codec(parser::Function, name::String) = register_codec(parser, name, V3Codec)

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
is_fixed_size(::BytesCodec) = true

register_codec("bytes", BytesCodec) do config, ctx
    endian_str = get(config, "endian", "little")
    endian = endian_str == "little" ? :little :
             endian_str == "big"    ? :big    :
             throw(ArgumentError("Unknown endian: \"$endian_str\""))
    BytesCodec(endian)
end

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
    validate_index_pipeline(p::V3Pipeline)

Verify that every codec in `p` satisfies `is_fixed_size`, i.e. produces output
whose byte size is determined by input structure alone (shape/element type),
never by input values.

Variable-size compressors are forbidden because the shard decoder must know the
exact encoded index size before decoding (to locate the index within the shard
bytes), and because the `:start` encoder re-encodes after shifting offsets and
relies on the encoded size being unchanged.
"""
function validate_index_pipeline(p::V3Pipeline)
    for codec in p.array_array
        is_fixed_size(codec) || throw(ArgumentError(
            "index_codecs array→array codec '$(name(codec))' ($(typeof(codec))) is not " *
            "fixed-size. Variable-size codecs would corrupt the shard index."
        ))
    end
    is_fixed_size(p.array_bytes) || throw(ArgumentError(
        "index_codecs array→bytes codec '$(name(p.array_bytes))' ($(typeof(p.array_bytes))) " *
        "is not fixed-size. Variable-size codecs would make the encoded index size unpredictable."
    ))
    for codec in p.bytes_bytes
        is_fixed_size(codec) || throw(ArgumentError(
            "index_codecs bytes→bytes codec '$(name(codec))' ($(typeof(codec))) is not " *
            "fixed-size. Variable-size compressors would corrupt the shard index."
        ))
    end
end

register_codec("sharding_indexed", ShardingCodec) do config, ctx
    N = length(config["chunk_shape"])
    # Zarr spec stores chunk_shape in C-order (row-major); reverse for Julia column-major
    chunk_shape    = NTuple{N,Int}(reverse(Int.(config["chunk_shape"])))
    data_pipeline  = getCodec(config["codecs"], ctx)
    # Index encodes UInt64 offset/nbytes pairs, so use a UInt64-specific context
    index_ctx      = (elsize = sizeof(UInt64),)
    index_pipeline = getCodec(config["index_codecs"], index_ctx)
    validate_index_pipeline(index_pipeline)
    index_location = Symbol(get(config, "index_location", "end"))
    ShardingCodec(chunk_shape, data_pipeline, index_pipeline, index_location)
end

"""
Build a `V3Pipeline` from a flat ordered list of `V3Codec` values.
The list must contain: zero or more array→array codecs, exactly one array→bytes codec,
then zero or more bytes→bytes codecs.
"""
function _codecs_to_v3pipeline(codecs::Vector{<:V3Codec})
    aa = V3Codec{:array, :array}[]
    ab::Union{Nothing,V3Codec{:array, :bytes}} = nothing
    bb = V3Codec{:bytes, :bytes}[]
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
    getCodec(d::Dict, ctx=nothing)

Deserialize a V3 codec from a JSON dict by dispatching through the `codec_parsers`
registry. `ctx` is an optional context value (e.g. a NamedTuple with `shape` and
`elsize`) forwarded to the registered parser.
"""
function getCodec(d::Dict, ctx=nothing)
    codec_name = d["name"]
    haskey(codec_parsers, codec_name) ||
        throw(ArgumentError("Zarr.jl does not support the $codec_name codec"))
    entry = codec_parsers[codec_name]
    config = get(d, "configuration", Dict{String,Any}())
    return entry.parser(config, ctx)::entry.return_type
end

"""
    getCodec(dicts::Vector, ctx=nothing)

Deserialize a list of V3 codec dicts into a `V3Pipeline`.
"""
function getCodec(dicts::Vector, ctx=nothing)
    return _codecs_to_v3pipeline([getCodec(d, ctx) for d in dicts])
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
ChunkShardInfo() = ChunkShardInfo(MAX_UINT64, MAX_UINT64)  # sentinel: empty chunk

"""
    ShardIndex{N}

Internal structure representing the shard index.
Stores chunk location info for an N-dimensional grid of chunks.
Empty chunks are marked with `ChunkShardInfo(MAX_UINT64, MAX_UINT64)`.
"""
struct ShardIndex{N}
    chunks::Array{ChunkShardInfo, N}
end

"""
    ShardIndex(chunks_per_shard::NTuple{N,Int})

Create an empty shard index with all chunks marked as empty.
"""
function ShardIndex(chunks_per_shard::NTuple{N,Int}) where N
    return ShardIndex{N}(fill(ChunkShardInfo(), chunks_per_shard))
end

"""
    get_chunk_slice(idx::ShardIndex, chunk_coords::NTuple{N,Int})

Get the byte range `(offset, offset+nbytes)` for a chunk, or `nothing` if the chunk is empty.
"""
function get_chunk_slice(idx::ShardIndex, chunk_coords::NTuple{N,Int}) where N
    info = idx.chunks[chunk_coords...]
    info.offset == MAX_UINT64 && info.nbytes == MAX_UINT64 && return nothing
    return (Int(info.offset), Int(info.offset + info.nbytes))
end

"""
    set_chunk_slice!(idx::ShardIndex, chunk_coords::NTuple{N,Int}, offset::Int, nbytes::Int)

Set the byte range for a chunk in the index.
"""
function set_chunk_slice!(idx::ShardIndex, chunk_coords::NTuple{N,Int}, offset::Int, nbytes::Int) where N
    idx.chunks[chunk_coords...] = ChunkShardInfo(UInt64(offset), UInt64(nbytes))
end

"""
    set_chunk_empty!(idx::ShardIndex, chunk_coords::NTuple{N,Int})

Mark a chunk as empty in the index.
"""
function set_chunk_empty!(idx::ShardIndex, chunk_coords::NTuple{N,Int}) where N
    idx.chunks[chunk_coords...] = ChunkShardInfo()
end

"""
    calculate_chunks_per_shard(shard_shape::NTuple{N,Int}, chunk_shape::NTuple{N,Int})

Calculate how many inner chunks fit in each dimension of a shard.
"""
function calculate_chunks_per_shard(shard_shape::NTuple{N,Int}, chunk_shape::NTuple{N,Int}) where N
    return ntuple(i -> cld(shard_shape[i], chunk_shape[i]), N)
end

"""
    get_chunk_slice_in_shard(chunk_coords::NTuple{N,Int}, chunk_shape::NTuple{N,Int}, shard_shape::NTuple{N,Int})

Get the array slice ranges for an inner chunk within a shard.
`chunk_coords` are 1-based indices into the grid of inner chunks.
"""
function get_chunk_slice_in_shard(chunk_coords::NTuple{N,Int}, chunk_shape::NTuple{N,Int}, shard_shape::NTuple{N,Int}) where N
    return ntuple(N) do i
        start_idx = (chunk_coords[i] - 1) * chunk_shape[i] + 1
        end_idx   = min(chunk_coords[i] * chunk_shape[i], shard_shape[i])
        start_idx:end_idx
    end
end

"""
    encode_shard_index(index::ShardIndex, c::ShardingCodec)

Encode the shard index using `c.index_codecs`.

The index is linearized in column-major order (matching `CartesianIndices` iteration)
with alternating offset/nbytes values per inner chunk:
`[chunk_0_offset, chunk_0_nbytes, chunk_1_offset, chunk_1_nbytes, ...]`
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

"""
    decode_shard_index(index_bytes::Vector{UInt8}, chunks_per_shard::NTuple{N,Int}, c::ShardingCodec)

Decode the shard index from bytes using `c.index_codecs`.

The bytes encode alternating offset/nbytes pairs in column-major order:
`[offset0, nbytes0, offset1, nbytes1, ...]`
"""
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

const _encoded_index_size_cache = Dict{Tuple{Tuple{Vararg{Int}},V3Pipeline},Int}()
const _encoded_index_size_cache_lock = ReentrantLock()

"""
    compute_encoded_index_size(chunks_per_shard::NTuple{N,Int}, c::ShardingCodec)

Compute the byte size of the encoded shard index.

Per the Zarr v3 spec, the initial index size is `16 * prod(chunks_per_shard)` bytes
(two `UInt64` values per inner chunk), transformed by each fixed-size index codec.
Results are cached by `(chunks_per_shard, c.index_codecs)` to avoid repeated encoding.
"""
function compute_encoded_index_size(chunks_per_shard::NTuple{N,Int}, c::ShardingCodec) where N
    key = (chunks_per_shard, c.index_codecs)
    Base.@lock _encoded_index_size_cache_lock begin
        return get!(_encoded_index_size_cache, key) do
            length(encode_shard_index(ShardIndex(chunks_per_shard), c))
        end
    end
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
        inner_shape   = ntuple(i -> length(slice_ranges[i]), N)
        # Per spec, partial inner chunks at shard edges must be padded to chunk_shape
        chunk_data = if inner_shape == c.chunk_shape
            data[slice_ranges...]
        else
            buf = zeros(eltype(data), c.chunk_shape)
            buf[ntuple(i -> 1:inner_shape[i], N)...] = data[slice_ranges...]
            buf
        end
        encoded_chunk = pipeline_encode(c.codecs, chunk_data, nothing)

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
        # validate_index_pipeline already enforces fixed-size codecs, so this must hold
        length(encoded_index) == index_size || error(
            "index_codecs produced different sizes on re-encoding ($(index_size) → " *
            "$(length(encoded_index))): only fixed-size codecs are permitted"
        )
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
    zdecode!(data, encoded, c::ShardingCodec, fill_value=nothing)

Decode sharded bytes into `data` (a full outer chunk / shard).
`fill_value` is used for empty or missing inner chunks; falls back to `zero(T)` when `nothing`.
"""
function zdecode!(data::AbstractArray, encoded::Vector{UInt8}, c::ShardingCodec{N}, fill_value=nothing) where N
    T  = eltype(data)
    fv = fill_value !== nothing ? fill_value : zero(T)

    if isempty(encoded)
        fill!(data, fv)
        return data
    end

    shard_shape      = size(data)
    chunks_per_shard = calculate_chunks_per_shard(shard_shape, c.chunk_shape)
    index_size       = compute_encoded_index_size(chunks_per_shard, c)

    if c.index_location == :start
        index_bytes = encoded[1:index_size]
    else
        index_bytes = encoded[end-index_size+1:end]
    end
    # Offsets stored in the index are absolute from the start of the shard:
    # - :end  — chunks occupy bytes [0, current_offset), index follows; offsets are 0-based from shard start.
    # - :start — encode shifted all offsets by index_size, so they are also absolute from shard start.
    chunk_data_offset = 0

    index = decode_shard_index(index_bytes, chunks_per_shard, c)

    for cart_idx in CartesianIndices(chunks_per_shard)
        chunk_coords = Tuple(cart_idx)
        array_slice  = get_chunk_slice_in_shard(chunk_coords, c.chunk_shape, shard_shape)
        chunk_slice  = get_chunk_slice(index, chunk_coords)

        if chunk_slice === nothing
            data[array_slice...] .= fv
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

"""
    sharding_codec(p) -> Union{Nothing, ShardingCodec}

If `p` is a "pure" sharding pipeline — no array→array codecs before
sharding and no bytes→bytes codecs after — return its inner
`ShardingCodec`. Otherwise return `nothing`.

Used by `ZArray.readblock!` to dispatch partial reads to a fast path
that decodes only the inner chunks intersecting the requested slice
instead of the whole shard. Pre/post codecs would have to be
reapplied per inner chunk, which is the general path and not yet
optimized.
"""
function sharding_codec(p::V3Pipeline)
    isempty(p.array_array) || return nothing
    isempty(p.bytes_bytes) || return nothing
    return p.array_bytes isa ShardingCodec ? p.array_bytes : nothing
end
sharding_codec(_) = nothing


"""
    _shard_index_byte_range(sc, total_size) -> UnitRange{Int}

Byte range, 1-based and inclusive, where the shard index lives inside a
shard file of size `total_size`.
"""
function _shard_index_byte_range(sc::ShardingCodec{N}, chunks_per_shard::NTuple{N,Int}, total_size::Int) where N
    index_size = compute_encoded_index_size(chunks_per_shard, sc)
    if sc.index_location == :start
        return 1:index_size
    else
        return (total_size - index_size + 1):total_size
    end
end


"""
    read_shard_partial_with_source!(aout, output_base_offsets, sc, index_bytes,
                                    fetch_inner_chunk_bytes,
                                    chunk_shape, current_chunk_offsets,
                                    indranges, fill_value)

Decode just the inner chunks of one outer (sharded) chunk that intersect
`indranges`, writing each intersection straight into the right slice of
`aout`. The caller supplies the already-read shard `index_bytes` and a
closure `fetch_inner_chunk_bytes(offset_from_shard_start, nbytes) ->
Vector{UInt8}` that returns the raw bytes for an inner chunk. This lets
the same loop drive both the in-memory case (slice a `chunk_compressed`
blob) and the byte-range storage case (`seek` + `read` the file).

Coordinate conventions:

- `indranges`             — global 1-based ranges, intersection of the
                            user request with this outer chunk
- `current_chunk_offsets` — 0-based global offsets of this outer chunk's
                            origin
- `output_base_offsets`   — 0-based global offsets of the user's request
                            (so `aout` indices = global −
                            `output_base_offsets`)
"""
function read_shard_partial_with_source!(
    aout::AbstractArray{T,N},
    output_base_offsets::NTuple{N,Int},
    sc::ShardingCodec{N},
    index_bytes::Vector{UInt8},
    fetch_inner_chunk_bytes::F,
    chunk_shape::NTuple{N,Int},
    current_chunk_offsets::NTuple{N,Int},
    indranges::NTuple{N,UnitRange{Int}},
    fill_value,
) where {T, N, F}
    fv = fill_value === nothing ? zero(T) : T(fill_value)

    local_indranges = ntuple(N) do i
        lo = first(indranges[i]) - current_chunk_offsets[i]
        hi = last(indranges[i])  - current_chunk_offsets[i]
        lo:hi
    end

    chunks_per_shard = calculate_chunks_per_shard(chunk_shape, sc.chunk_shape)
    index = decode_shard_index(index_bytes, chunks_per_shard, sc)

    # Build the work list of intersecting inner chunks up front, then
    # decode them. Each entry's output region is disjoint by
    # construction (different ic_coords → non-overlapping aout_dest), so
    # the decode loop is safe to parallelize when threads are available.
    Work = Tuple{NTuple{N,UnitRange{Int}},
                 NTuple{N,UnitRange{Int}},
                 Union{Nothing,Tuple{Int,Int}}}
    work = Work[]
    for cart_idx in CartesianIndices(chunks_per_shard)
        ic_coords      = Tuple(cart_idx)
        ic_array_slice = get_chunk_slice_in_shard(ic_coords, sc.chunk_shape, chunk_shape)
        intersection = ntuple(N) do i
            lo = max(first(ic_array_slice[i]), first(local_indranges[i]))
            hi = min(last(ic_array_slice[i]),  last(local_indranges[i]))
            lo:hi
        end
        any(isempty, intersection) && continue

        ic_local = ntuple(N) do i
            lo = first(intersection[i]) - first(ic_array_slice[i]) + 1
            hi = last(intersection[i])  - first(ic_array_slice[i]) + 1
            lo:hi
        end
        aout_dest = ntuple(N) do i
            lo = first(intersection[i]) + current_chunk_offsets[i] - output_base_offsets[i]
            hi = last(intersection[i])  + current_chunk_offsets[i] - output_base_offsets[i]
            lo:hi
        end

        cs = get_chunk_slice(index, ic_coords)
        push!(work, (ic_local, aout_dest, cs === nothing ? nothing : (Int(cs[1]), Int(cs[2]))))
    end

    if isempty(work)
        return aout
    end

    # Run sequentially when threading wouldn't help (single thread, one
    # task, or the user has explicitly turned the knob off).
    use_threads = Threads.nthreads() > 1 &&
                  length(work) > 1 &&
                  enable_threaded_shard_decode[]

    if !use_threads
        inner_buf = Array{T}(undef, sc.chunk_shape)
        for (ic_local, aout_dest, cs) in work
            if cs === nothing
                view(aout, aout_dest...) .= fv
            else
                offset_start, offset_end = cs
                encoded_chunk = fetch_inner_chunk_bytes(offset_start, offset_end - offset_start)
                pipeline_decode!(sc.codecs, inner_buf, encoded_chunk)
                copyto!(view(aout, aout_dest...), view(inner_buf, ic_local...))
            end
        end
        return aout
    end

    # Threaded path: bounded buffer pool so we don't allocate one
    # full-shard buffer per task. min(nthreads, |work|) buffers is
    # enough — each task takes one off the channel, decodes, returns it.
    n_workers = min(Threads.nthreads(), length(work))
    pool = Channel{Array{T,N}}(n_workers)
    for _ in 1:n_workers
        put!(pool, Array{T,N}(undef, sc.chunk_shape))
    end

    @sync for (ic_local, aout_dest, cs) in work
        Threads.@spawn begin
            if cs === nothing
                view(aout, aout_dest...) .= fv
            else
                offset_start, offset_end = cs
                buf = take!(pool)
                try
                    encoded_chunk = fetch_inner_chunk_bytes(offset_start, offset_end - offset_start)
                    pipeline_decode!(sc.codecs, buf, encoded_chunk)
                    copyto!(view(aout, aout_dest...), view(buf, ic_local...))
                finally
                    put!(pool, buf)
                end
            end
        end
    end
    return aout
end


"""
    read_shard_partial!(aout, output_base_offsets, sc, chunk_compressed,
                        chunk_shape, current_chunk_offsets, indranges,
                        fill_value)

In-memory variant: caller has already loaded the entire shard into
`chunk_compressed`. Pulls the index out of that blob and forwards to
`read_shard_partial_with_source!` with a closure that slices the blob
for each inner chunk's bytes. `nothing`/empty `chunk_compressed`
fills the request region with `fill_value` and returns.
"""
function read_shard_partial!(
    aout::AbstractArray{T,N},
    output_base_offsets::NTuple{N,Int},
    sc::ShardingCodec{N},
    chunk_compressed::Union{Vector{UInt8}, Nothing},
    chunk_shape::NTuple{N,Int},
    current_chunk_offsets::NTuple{N,Int},
    indranges::NTuple{N,UnitRange{Int}},
    fill_value,
) where {T, N}
    if chunk_compressed === nothing || isempty(chunk_compressed)
        fv = fill_value === nothing ? zero(T) : T(fill_value)
        aout_dest_outer = ntuple(N) do i
            lo = first(indranges[i]) - output_base_offsets[i]
            hi = last(indranges[i])  - output_base_offsets[i]
            lo:hi
        end
        view(aout, aout_dest_outer...) .= fv
        return aout
    end

    chunks_per_shard = calculate_chunks_per_shard(chunk_shape, sc.chunk_shape)
    idx_range = _shard_index_byte_range(sc, chunks_per_shard, length(chunk_compressed))
    index_bytes = chunk_compressed[idx_range]

    fetch = (off::Int, nb::Int) -> chunk_compressed[(off + 1):(off + nb)]

    return read_shard_partial_with_source!(
        aout, output_base_offsets, sc, index_bytes, fetch,
        chunk_shape, current_chunk_offsets, indranges, fill_value,
    )
end


struct TransposeCodec{N} <: V3Codec{:array, :array}
    order::NTuple{N, Int}  # permutation (1-based Julia indexing)
end
name(::TransposeCodec) = "transpose"
is_fixed_size(::TransposeCodec) = true

register_codec("transpose", TransposeCodec) do config, ctx
    _order = config["order"]
    if _order isa AbstractString
        n = isnothing(ctx) ? error("context with shape required for deprecated string transpose order") : length(ctx.shape)
        if _order == "C"
            @warn "Transpose codec dimension order of C is deprecated"
            perm = ntuple(identity, n)
        elseif _order == "F"
            @warn "Transpose codec dimension order of F is deprecated"
            perm = ntuple(i -> n - i + 1, n)
        else
            throw(ArgumentError("Unknown transpose order string: $_order"))
        end
    else
        perm = Tuple(Int.(_order) .+ 1)
    end
    TransposeCodec(perm)
end

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

function codec_decode(c::BytesCodec, encoded::Vector{UInt8}, ::Type{T}, shape::NTuple{N,Int}; fill_value=nothing) where {T, N}
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

function codec_decode(c::ShardingCodec, encoded::Vector{UInt8}, ::Type{T}, shape::NTuple{N,Int}; fill_value=nothing) where {T, N}
    output = Array{T, N}(undef, shape)
    zdecode!(output, encoded, c, fill_value)
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

register_codec("gzip", GzipV3Codec) do config, ctx
    GzipV3Codec(get(config, "level", 6))
end

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

register_codec("blosc", BloscV3Codec) do config, ctx
    cname = get(config, "cname", "lz4")
    clevel = get(config, "clevel", 5)
    shuffle_val = get(config, "shuffle", "noshuffle")
    shuffle_int = shuffle_val isa Integer ? shuffle_val :
                  shuffle_val == "noshuffle"  ? 0 :
                  shuffle_val == "shuffle"     ? 1 :
                  shuffle_val == "bitshuffle"  ? 2 :
                  throw(ArgumentError("Unknown shuffle: \"$shuffle_val\"."))
    blocksize = get(config, "blocksize", 0)
    typesize_default = isnothing(ctx) ? 4 : ctx.elsize
    typesize = get(config, "typesize", typesize_default)
    BloscV3Codec(string(cname), clevel, shuffle_int, blocksize, typesize)
end

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

register_codec("zstd", ZstdV3Codec) do config, ctx
    ZstdV3Codec(get(config, "level", 3))
end

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
is_fixed_size(::CRC32cV3Codec) = true

register_codec("crc32c", CRC32cV3Codec) do config, ctx
    CRC32cV3Codec()
end

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
