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

function pipeline_decode!(p::V3Pipeline, output::AbstractArray, compressed::Vector{UInt8}; fill_value=nothing)
    # Phase 3 reverse: bytes->bytes codecs (reverse order)
    bytes = compressed
    for codec in reverse(collect(p.bytes_bytes))
        bytes = Codecs.V3Codecs.codec_decode(codec, bytes)
    end
    # Phase 2 reverse: bytes->array codec
    # Compute the intermediate shape — the shape data has after array_array encoding
    intermediate_shape = foldl(
        (sz, codec) -> Codecs.V3Codecs.encoded_shape(codec, sz),
        p.array_array; init=size(output)
    )
    arr = Codecs.V3Codecs.codec_decode(p.array_bytes, bytes, eltype(output), intermediate_shape; fill_value)
    # Phase 1 reverse: array->array codecs (reverse order)
    for codec in reverse(collect(p.array_array))
        arr = Codecs.V3Codecs.codec_decode(codec, arr)
    end
    copyto!(output, arr)
    return output
end

# Convenience: extract pipeline from metadata
get_pipeline(m::MetadataV2) = V2Pipeline(m.compressor, m.filters)
get_pipeline(m::MetadataV3) = m.pipeline
