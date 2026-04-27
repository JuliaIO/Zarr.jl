function pipeline_encode(p::V2Pipeline, data::AbstractArray, fill_value)
    if fill_value !== nothing && all(isequal(fill_value), data)
        return nothing
    end
    dtemp = UInt8[]
    zcompress!(dtemp, data, p.compressor, p.filters)
    return dtemp
end

function pipeline_decode!(p::V2Pipeline, output::AbstractArray, compressed::Vector{UInt8}; fill_value=nothing)
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
    # Compute the byte size of the array-bytes step's output — equal to
    # the (post-array_array) decoded element count × sizeof(eltype). For
    # the common sharded inner-chunk path this is exactly the inner chunk
    # in bytes, which lets us route bytes-bytes codecs through a single
    # reusable buffer and the final array-bytes step straight into
    # `output` with no intermediate Array{T} allocation.
    intermediate_shape = foldl(
        (sz, codec) -> Codecs.V3Codecs.encoded_shape(codec, sz),
        p.array_array; init=size(output)
    )
    decoded_byte_size = prod(intermediate_shape) * sizeof(eltype(output))

    # Walk the bytes-bytes chain in reverse. The last step's output must
    # be exactly `decoded_byte_size`; intermediate steps run via the
    # allocating `codec_decode` (their output sizes vary per codec and
    # aren't worth pre-sizing for the rare multi-bytes-codec case).
    n_bb = length(p.bytes_bytes)
    if isempty(p.array_array) && n_bb >= 1
        # Common case: pipeline ends in [array_bytes] and starts with
        # one or more bytes-bytes codecs. The first reverse step (= last
        # forward step) gets the final-sized output buffer; downstream
        # steps cascade through `codec_decode`. For the dominant case
        # `[BytesCodec, ZstdCompressor]` (n_bb == 1), this collapses to
        # one in-place zstd decode + one in-place BytesCodec decode and
        # zero chunk-sized intermediate allocations.
        bytes_buf = Vector{UInt8}(undef, decoded_byte_size)
        bytes = compressed
        bb = collect(p.bytes_bytes)
        for i in length(bb):-1:1
            codec = bb[i]
            if i == 1
                Codecs.V3Codecs.codec_decode!(codec, bytes_buf, bytes)
            else
                bytes = Codecs.V3Codecs.codec_decode(codec, bytes)
            end
        end
        Codecs.V3Codecs.codec_decode!(p.array_bytes, output, bytes_buf; fill_value)
        return output
    end

    # Fallback path: array_array codecs present (e.g. transpose) or no
    # bytes-bytes codecs. Same logic as before, allocating where
    # necessary.
    bytes = compressed
    for codec in reverse(collect(p.bytes_bytes))
        bytes = Codecs.V3Codecs.codec_decode(codec, bytes)
    end
    if isempty(p.array_array)
        Codecs.V3Codecs.codec_decode!(p.array_bytes, output, bytes; fill_value)
        return output
    end
    arr = Codecs.V3Codecs.codec_decode(p.array_bytes, bytes, eltype(output), intermediate_shape; fill_value)
    for codec in reverse(collect(p.array_array))
        arr = Codecs.V3Codecs.codec_decode(codec, arr)
    end
    copyto!(output, arr)
    return output
end

# Convenience: extract pipeline from metadata
get_pipeline(m::MetadataV2) = V2Pipeline(m.compressor, m.filters)
get_pipeline(m::MetadataV3) = m.pipeline
