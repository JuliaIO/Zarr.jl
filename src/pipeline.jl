abstract type AbstractCodecPipeline end

"""
V2Pipeline wraps the existing v2 compressor + filter pair.
Delegates to zcompress!/zuncompress! with zero behavior change.
"""
struct V2Pipeline{C<:Compressor, F} <: AbstractCodecPipeline
    compressor::C
    filters::F
end

function pipeline_encode(p::V2Pipeline, data::AbstractArray, fill_value)
    if fill_value !== nothing && all(isequal(fill_value), data)
        return nothing
    end
    dtemp = UInt8[]
    zcompress!(dtemp, data, p.compressor, p.filters)
    return dtemp
end

function pipeline_decode!(p::V2Pipeline, output::AbstractArray, compressed::Vector{UInt8})
    zuncompress!(output, compressed, p.compressor, p.filters)
    return output
end

"""
V3Pipeline holds a three-phase v3 codec chain:
- array_array: tuple of array->array codecs (e.g. transpose)
- array_bytes: single array->bytes codec (e.g. bytes, sharding)
- bytes_bytes: tuple of bytes->bytes codecs (e.g. gzip, blosc, crc32c)
"""
struct V3Pipeline{AA, AB, BB} <: AbstractCodecPipeline
    array_array::AA
    array_bytes::AB
    bytes_bytes::BB
end

function pipeline_encode(p::V3Pipeline, data::AbstractArray, fill_value)
    if fill_value !== nothing && all(isequal(fill_value), data)
        return nothing
    end
    # Phase 1: array->array codecs (forward order)
    result = data
    for codec in p.array_array
        result = Codecs.V3Codecs.codec_encode(codec, result)
    end
    # Phase 2: array->bytes codec
    bytes = Codecs.V3Codecs.codec_encode(p.array_bytes, result)
    # Phase 3: bytes->bytes codecs (forward order)
    for codec in p.bytes_bytes
        bytes = Codecs.V3Codecs.codec_encode(codec, bytes)
    end
    return bytes
end

function pipeline_decode!(p::V3Pipeline, output::AbstractArray, compressed::Vector{UInt8})
    # Phase 3 reverse: bytes->bytes codecs (reverse order)
    bytes = compressed
    for codec in reverse(collect(p.bytes_bytes))
        bytes = Codecs.V3Codecs.codec_decode(codec, bytes)
    end
    # Phase 2 reverse: bytes->array codec
    arr = Codecs.V3Codecs.codec_decode(p.array_bytes, bytes, eltype(output), size(output))
    # Phase 1 reverse: array->array codecs (reverse order)
    for codec in reverse(collect(p.array_array))
        arr = Codecs.V3Codecs.codec_decode(codec, arr)
    end
    copyto!(output, arr)
    return output
end

# Convenience: extract pipeline from metadata
get_pipeline(m::MetadataV2) = V2Pipeline(m.compressor, m.filters)
