"""
Prototype Zarr version 3 support
"""

const typemap3 = Dict{String, DataType}()
foreach([Bool, Int8, Int16, Int32, Int64, UInt8, UInt16, UInt32, UInt64, Float16, Float32, Float64]) do t
    typemap3[lowercase(string(t))] = t
end
typemap3["complex64"] = ComplexF32
typemap3["complex128"] = ComplexF64

function typestr3(t::Type)
    return lowercase(string(t))
end
# TODO: Check raw types
function typestr3(::Type{NTuple{N,UInt8}}) where {N}
    return "r$(N*8)"
end

function typestr3(s::AbstractString, codecs=nothing)
    if !haskey(typemap3, s)
        if startswith(s, "r")
            num_bits = tryparse(Int, s[2:end])
            if isnothing(num_bits)
                throw(ArgumentError("$s is not a known type"))
            end
            if mod(num_bits, 8) == 0
                return NTuple{num_bits÷8,UInt8}
            else
                throw(ArgumentError("$s must describe a raw type with bit size that is a multiple of 8 bits"))
            end
        end
    end
    return typemap3[s]
end

function check_keys(d::AbstractDict, keys)
    for key in keys
        if !haskey(d, key)
            throw(ArgumentError("Zarr v3 metadata must have a key called $key"))
        end
    end
end

"""Metadata for Zarr version 3 arrays"""
struct MetadataV3{T,N,P<:AbstractCodecPipeline,E<:AbstractChunkKeyEncoding} <: AbstractMetadata{T,N,E}
    zarr_format::Int
    node_type::String
    shape::Base.RefValue{NTuple{N, Int}}
    chunks::NTuple{N, Int}
    dtype::String  # data_type in v3
    pipeline::P
    fill_value::Union{T, Nothing}
    chunk_key_encoding::E
    function MetadataV3{T2,N,P,E}(zarr_format, node_type, shape, chunks, dtype, pipeline, fill_value, chunk_key_encoding) where {T2,N,P,E}
        zarr_format == 3 || throw(ArgumentError("MetadataV3 only functions if zarr_format == 3"))
        #Do some sanity checks to make sure we have a sane array
        any(<(0), shape) && throw(ArgumentError("Size must be positive"))
        any(<(1), chunks) && throw(ArgumentError("Chunk size must be >= 1 along each dimension"))
        new{T2,N,P,E}(zarr_format, node_type, Base.RefValue{NTuple{N,Int}}(shape), chunks, dtype, pipeline, fill_value, chunk_key_encoding)
    end
end
MetadataV3{T2,N,P}(args...) where {T2,N,P} = MetadataV3{T2,N,P,ChunkKeyEncoding}(args...)
zarr_format(::MetadataV3) = ZarrFormat(Val(3))

"""
Convenience constructor for MetadataV3 that builds the codec pipeline from
`order` (translated to a TransposeCodec), `endian` (translated to a BytesCodec),
and `compressor` (translated to bytes->bytes codecs).
"""
function MetadataV3{T2,N}(zarr_format, node_type, shape::NTuple{N,Int}, chunks::NTuple{N,Int},
        dtype::String, fill_value;
        order::Char='C',
        endian::Symbol=:little,
        compressor=BloscCompressor(),
        chunk_key_encoding::E=ChunkKeyEncoding('/', true)
    ) where {T2, N, E}
    T_base = Base.nonmissingtype(T2)
    array_array_codecs = if order == 'F'
        (Codecs.V3Codecs.TransposeCodec(ntuple(i -> N - i + 1, N)),)
    else
        ()
    end
    array_bytes_codec = Codecs.V3Codecs.BytesCodec(endian)
    bytes_bytes_codecs = if compressor isa NoCompressor
        ()
    elseif compressor isa BloscCompressor
        (Codecs.V3Codecs.BloscV3Codec(compressor.cname, compressor.clevel, compressor.shuffle, compressor.blocksize, sizeof(T_base)),)
    elseif compressor isa ZlibCompressor
        # ZlibCompressor uses -1 to mean "default"; zarr v3 gzip spec requires 0-9
        level = compressor.config.level == -1 ? 6 : compressor.config.level
        (Codecs.V3Codecs.GzipV3Codec(level),)
    elseif compressor isa ZstdCompressor
        (Codecs.V3Codecs.ZstdV3Codec(compressor.config.compressionLevel),)
    else
        throw(ArgumentError("Unsupported compressor type for v3: $(typeof(compressor))"))
    end
    pipeline = V3Pipeline(array_array_codecs, array_bytes_codec, bytes_bytes_codecs)
    return MetadataV3{T2,N,typeof(pipeline),E}(zarr_format, node_type, shape, chunks, dtype, pipeline, fill_value, chunk_key_encoding)
end

function Base.:(==)(m1::MetadataV3, m2::MetadataV3)
  m1.zarr_format == m2.zarr_format &&
  m1.node_type == m2.node_type &&
  m1.shape[] == m2.shape[] &&
  m1.chunks == m2.chunks &&
  m1.dtype == m2.dtype &&
  m1.fill_value == m2.fill_value &&
  m1.pipeline == m2.pipeline &&
  m1.chunk_key_encoding == m2.chunk_key_encoding
end

"""
Derive the storage order ('C' or 'F') from the codec pipeline of a MetadataV3.

Throws `ArgumentError` if the order cannot be unambiguously determined, which
occurs when:
- the pipeline contains more than one array->array codec,
- an array->array codec is not a `TransposeCodec` (unknown effect on order), or
- the `TransposeCodec` permutation is neither the identity (C order) nor the
  full reversal (F order).
"""
function get_order(md::MetadataV3)
    array_array = md.pipeline.array_array
    if length(array_array) == 0
        return 'C'
    end
    if length(array_array) > 1
        throw(ArgumentError(
            "Cannot determine storage order: pipeline has $(length(array_array)) " *
            "array->array codecs; composed permutations yield an indeterminate order"
        ))
    end
    codec = only(array_array)
    if !(codec isa Codecs.V3Codecs.TransposeCodec)
        throw(ArgumentError(
            "Cannot determine storage order: unrecognized array->array codec $(typeof(codec))"
        ))
    end
    N = ndims(md)
    c_perm  = ntuple(identity, N)
    f_perm  = ntuple(i -> N - i + 1, N)
    if codec.order == c_perm
        return 'C'
    elseif codec.order == f_perm
        return 'F'
    else
        throw(ArgumentError(
            "Cannot determine storage order: TransposeCodec permutation $(codec.order) " *
            "is neither C order $c_perm nor F order $f_perm"
        ))
    end
end
get_order(md::MetadataV2) = md.order



function Metadata3(d::AbstractDict, fill_as_missing)
    check_keys(d, ("zarr_format", "node_type"))

    zarr_format = d["zarr_format"]::Int

    node_type = d["node_type"]::String
    if node_type ∉ ("group", "array")
        throw(ArgumentError("Unknown node_type of $node_type"))
    end

    zarr_format == 3 || throw(ArgumentError("Metadata3 only functions if zarr_format == 3"))

    # Groups
    if node_type == "group"
        # Groups only need zarr_format and node_type
        # Optionally they can have attributes
        for key in keys(d)
            if key ∉ ("zarr_format", "node_type", "attributes")
            if d[key]["must_understand"] == false
                @warn "Zarr v3 group metadata has an unrecognized key called $key with must_understand=false; ignoring"
            else
                throw(ArgumentError("Zarr v3 group metadata has an unrecognized key called $key with must_understand=true"))
            end
            end
        end

        group_pipeline = V3Pipeline((), Codecs.V3Codecs.BytesCodec(), ())
        return MetadataV3{Int,0,typeof(group_pipeline),ChunkKeyEncoding}(zarr_format, node_type, (), (), "", group_pipeline, 0, ChunkKeyEncoding('/', true))
    end

    # Array keys
    mandatory_keys = [
        "zarr_format",
        "node_type",
        "shape",
        "data_type",
        "chunk_grid",
        "chunk_key_encoding",
        "fill_value",
        "codecs",
    ]
    optional_keys = [
        "attributes",
        "storage_transformers",
        "dimension_names",
    ]

    check_keys(d, mandatory_keys)
    for key in keys(d)
        if key ∉ mandatory_keys && key ∉ optional_keys
            if d[key]["must_understand"] === false
                @warn "Zarr v3 array metadata has an unrecognized key called $key with must_understand=false; ignoring"
            else                
                throw(ArgumentError("Zarr v3 array metadata has an unrecognized key called $key with must_understand=true"))
            end
            #throw(ArgumentError("Zarr v3 metadata cannot have a key called $key"))
        end
    end

    # Shape
    shape = Int.(d["shape"])

    # Datatype
    data_type = d["data_type"]::String

    # Chunk Grid
    chunk_grid = d["chunk_grid"]
    if chunk_grid["name"] == "regular"
        chunks = Int.(chunk_grid["configuration"]["chunk_shape"])
        if length(shape) != length(chunks)
            throw(ArgumentError("Shape has rank $(length(shape)) which does not match the chunk_shape rank of $(length(chunks))"))
        end
    else
        throw(ArgumentError("Unknown chunk_grid of name, $(chunk_grid["name"])"))
    end

    # Chunk Key Encoding
    chunk_key_encoding = d["chunk_key_encoding"]

    # Build V3Pipeline from codec chain
    array_array_codecs = []
    array_bytes_codec = nothing
    bytes_bytes_codecs = []
    order = 'C'  # default

    for codec in d["codecs"]
        codec_name = codec["name"]
        config = get(codec, "configuration", Dict{String,Any}())
        if codec_name == "transpose"
            _order = config["order"]
            if _order isa AbstractString
                n = length(shape)
                if _order == "C"
                    @warn "Transpose codec dimension order of C is deprecated"
                    perm = ntuple(identity, n)
                elseif _order == "F"
                    @warn "Transpose codec dimension order of F is deprecated"
                    perm = ntuple(i -> n - i + 1, n)
                    order = 'F'
                else
                    throw(ArgumentError("Unknown transpose order string: $_order"))
                end
            else
                perm = Tuple(Int.(_order) .+ 1)
                default_perm = ntuple(identity, length(shape))
                rev_perm = ntuple(i -> length(shape) - i + 1, length(shape))
                if perm == default_perm
                    order = 'C'
                elseif perm == rev_perm
                    order = 'F'
                end
            end
            push!(array_array_codecs, Codecs.V3Codecs.TransposeCodec(perm))
        elseif codec_name == "bytes"
            endian_str = get(config, "endian", "little")
            endian = endian_str == "little" ? :little :
                     endian_str == "big"    ? :big    :
                     throw(ArgumentError("Unknown endian value: \"$endian_str\""))
            array_bytes_codec = Codecs.V3Codecs.BytesCodec(endian)
        elseif codec_name == "sharding_indexed"
            throw(ArgumentError("Zarr.jl currently does not support the sharding_indexed codec"))
        elseif codec_name == "gzip"
            level = get(config, "level", 6)
            push!(bytes_bytes_codecs, Codecs.V3Codecs.GzipV3Codec(level))
        elseif codec_name == "blosc"
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
            push!(bytes_bytes_codecs, Codecs.V3Codecs.BloscV3Codec(string(cname), clevel, shuffle_int, blocksize, typesize))
        elseif codec_name == "zstd"
            level = get(config, "level", 3)
            push!(bytes_bytes_codecs, Codecs.V3Codecs.ZstdV3Codec(level))
        elseif codec_name == "crc32c"
            push!(bytes_bytes_codecs, Codecs.V3Codecs.CRC32cV3Codec())
        else
            throw(ArgumentError("Zarr.jl currently does not support the $codec_name codec"))
        end
    end

    isnothing(array_bytes_codec) && throw(ArgumentError("V3 codec chain must contain a 'bytes' codec"))
    pipeline = V3Pipeline(Tuple(array_array_codecs), array_bytes_codec, Tuple(bytes_bytes_codecs))

    # Type Parameters
    T = typestr3(data_type)
    N = length(shape)

    fv = fill_value_decoding(d["fill_value"], T)::T

    TU = (fv === nothing || !fill_as_missing) ? T : Union{T,Missing}

    chunk_key_encoding = parse_chunk_key_encoding(chunk_key_encoding)
    E = typeof(chunk_key_encoding)

    MetadataV3{TU, N, typeof(pipeline), E}(
        zarr_format,
        node_type,
        NTuple{N, Int}(shape) |> reverse,
        NTuple{N, Int}(chunks) |> reverse,
        data_type,
        pipeline,
        fv,
        chunk_key_encoding,
    )
end

"Construct MetadataV3 based on your data"
function Metadata3(A::AbstractArray{T, N}, chunks::NTuple{N, Int};
        node_type::String="array",
        compressor=BloscCompressor(),
        fill_value::Union{T, Nothing}=nothing,
        order::Char='C',
        endian::Symbol=:little,
        filters=nothing,
        fill_as_missing = false,
        dimension_separator::Char = '/'
    ) where {T, N}
    @warn("Zarr v3 support is experimental")
    T2 = (fill_value === nothing || !fill_as_missing) ? T : Union{T,Missing}
    if fill_value === nothing
        fill_value = zero(T)
    end
    return MetadataV3{T2, N}(
        3,
        node_type,
        size(A),
        chunks,
        typestr3(eltype(A)),
        fill_value;
        order=order,
        endian=endian,
        compressor=compressor,
        chunk_key_encoding=ChunkKeyEncoding(dimension_separator, true)
    )
end

function lower3(md::MetadataV3{T}) where T
    chunk_grid = Dict{String,Any}(
        "name" => "regular",
        "configuration" => Dict{String,Any}(
            "chunk_shape" => md.chunks |> reverse
        )
    )

    # chunk_key_encoding
    chunk_key_encoding = lower_chunk_key_encoding(md.chunk_key_encoding)

    # Build codecs from pipeline
    codecs = Dict{String,Any}[]
    p = md.pipeline

    # array->array codecs
    for codec in p.array_array
        if codec isa Codecs.V3Codecs.TransposeCodec
            push!(codecs, Dict{String,Any}(
                "name" => "transpose",
                "configuration" => Dict("order" => collect(codec.order .- 1))
            ))
        end
    end

    # array->bytes codec
    if p.array_bytes isa Codecs.V3Codecs.BytesCodec
        push!(codecs, Dict{String,Any}(
            "name" => "bytes",
            "configuration" => Dict{String,Any}("endian" => string(p.array_bytes.endian))
        ))
    end

    # bytes->bytes codecs
    for codec in p.bytes_bytes
        if codec isa Codecs.V3Codecs.GzipV3Codec
            push!(codecs, Dict{String,Any}(
                "name" => "gzip",
                "configuration" => Dict{String,Any}("level" => codec.level)
            ))
        elseif codec isa Codecs.V3Codecs.BloscV3Codec
            push!(codecs, Dict{String,Any}(
                "name" => "blosc",
                "configuration" => Dict{String,Any}(
                    "cname" => codec.cname,
                    "clevel" => codec.clevel,
                    "shuffle" => codec.shuffle == 0 ? "noshuffle" :
                                 codec.shuffle == 1 ? "shuffle" :
                                 codec.shuffle == 2 ? "bitshuffle" :
                                 throw(ArgumentError("Unknown shuffle integer: $(codec.shuffle)")),
                    "blocksize" => codec.blocksize,
                    "typesize" => codec.typesize
                )
            ))
        elseif codec isa Codecs.V3Codecs.ZstdV3Codec
            push!(codecs, Dict{String,Any}(
                "name" => "zstd",
                "configuration" => Dict{String,Any}("level" => codec.level)
            ))
        elseif codec isa Codecs.V3Codecs.CRC32cV3Codec
            push!(codecs, Dict{String,Any}("name" => "crc32c"))
        end
    end

    Dict{String, Any}(
        "zarr_format" => Int(md.zarr_format),
        "node_type" => md.node_type,
        "shape" => md.shape[] |> reverse,
        "data_type" => typestr3(T),
        "chunk_grid" => chunk_grid,
        "chunk_key_encoding" => chunk_key_encoding,
        "fill_value" => fill_value_encoding(md.fill_value),
        "codecs" => codecs
    )
end

function Metadata(A::AbstractArray{T,N}, chunks::NTuple{N,Int}, ::ZarrFormat{3};
        node_type::String="array",
        compressor::C=BloscCompressor(),
        fill_value::Union{T, Nothing}=nothing,
        order::Char='C',
        endian::Symbol=:little,
        filters::F=nothing,
        fill_as_missing = false,
        chunk_key_encoding::E=ChunkKeyEncoding('/', true)
    ) where {T, N, C, F, E}
    return Metadata3(A, chunks;
        node_type=node_type,
        compressor=compressor,
        fill_value=fill_value,
        order=order,
        endian=endian,
        filters=filters,
        fill_as_missing=fill_as_missing,
        dimension_separator=chunk_key_encoding.sep
    )
end

# V3 constructor from Dict - delegate to Metadata3
function Metadata(d::AbstractDict, fill_as_missing, ::ZarrFormat{3})
    return Metadata3(d, fill_as_missing)
end

function JSON.lower(md::MetadataV3)
    return lower3(md)
end
