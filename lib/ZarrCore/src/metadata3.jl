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

# --- Registry-based codec parsing ---

function parse_v3_codec(codec_name::String, config::Dict, shape_length::Int)
    if codec_name == "transpose"
        return _parse_transpose_codec(config, shape_length)
    end
    haskey(Codecs.V3Codecs.v3_codec_parsers, codec_name) ||
        throw(ArgumentError("Zarr.jl currently does not support the $codec_name codec"))
    return Codecs.V3Codecs.v3_codec_parsers[codec_name](config)
end

function _parse_transpose_codec(config::Dict, shape_length::Int)
    _order = config["order"]
    if _order isa AbstractString
        n = shape_length
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
    return Codecs.V3Codecs.TransposeCodec(perm)
end

# --- Metadata3 from Dict ---

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
                throw(ArgumentError("Zarr v3 group metadata cannot have a key called $key"))
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
            throw(ArgumentError("Zarr v3 metadata cannot have a key called $key"))
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

    # Build V3Pipeline from codec chain using registry-based parsing
    array_array_codecs = []
    array_bytes_codec = nothing
    bytes_bytes_codecs = []

    for codec in d["codecs"]
        codec_name = codec["name"]
        config = get(codec, "configuration", Dict{String,Any}())
        parsed = parse_v3_codec(codec_name, config, length(shape))
        cat = Codecs.V3Codecs.codec_category(parsed)
        if cat == (:array, :array)
            push!(array_array_codecs, parsed)
        elseif cat == (:array, :bytes)
            array_bytes_codec = parsed
        elseif cat == (:bytes, :bytes)
            push!(bytes_bytes_codecs, parsed)
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

# --- Metadata3 from Array (takes bytes_bytes_codecs directly) ---

"Construct MetadataV3 based on your data"
function Metadata3(A::AbstractArray{T, N}, chunks::NTuple{N, Int};
        node_type::String="array",
        fill_value::Union{T, Nothing}=nothing,
        order::Char='C',
        endian::Symbol=:little,
        compressor=nothing,
        bytes_bytes_codecs::Tuple=(compressor === nothing ? () : compressor_to_v3_bytes_codecs(compressor)),
        fill_as_missing = false,
        dimension_separator::Char = '/'
    ) where {T, N}
    @warn("Zarr v3 support is experimental")
    T2 = (fill_value === nothing || !fill_as_missing) ? T : Union{T,Missing}
    if fill_value === nothing
        fill_value = zero(T)
    end
    T_base = Base.nonmissingtype(T2)
    array_array_codecs = if order == 'F'
        (Codecs.V3Codecs.TransposeCodec(ntuple(i -> N - i + 1, N)),)
    else
        ()
    end
    array_bytes_codec = Codecs.V3Codecs.BytesCodec(endian)
    pipeline = V3Pipeline(array_array_codecs, array_bytes_codec, bytes_bytes_codecs)
    chunk_key_encoding = ChunkKeyEncoding(dimension_separator, true)
    return MetadataV3{T2, N, typeof(pipeline), typeof(chunk_key_encoding)}(
        3,
        node_type,
        size(A),
        chunks,
        typestr3(eltype(A)),
        pipeline,
        fill_value,
        chunk_key_encoding,
    )
end

# --- Compressor to V3 bytes codecs dispatch ---

"""
    compressor_to_v3_bytes_codecs(compressor) -> Tuple

Convert a compressor to a tuple of V3 bytes->bytes codecs.
Subtypes should add methods for their specific compressor types.
"""
compressor_to_v3_bytes_codecs(::NoCompressor) = ()
compressor_to_v3_bytes_codecs(c) = throw(ArgumentError("Unsupported compressor type for v3: $(typeof(c)). Register a method for compressor_to_v3_bytes_codecs."))

# --- Metadata(A, chunks, ::ZarrFormat{3}) using compressor_to_v3_bytes_codecs ---

function Metadata(A::AbstractArray{T,N}, chunks::NTuple{N,Int}, ::ZarrFormat{3};
        node_type::String="array",
        compressor=default_compressor(),
        fill_value::Union{T, Nothing}=nothing,
        order::Char='C',
        endian::Symbol=:little,
        filters=nothing,
        fill_as_missing = false,
        chunk_key_encoding::E=ChunkKeyEncoding('/', true)
    ) where {T, N, E}
    bb_codecs = compressor_to_v3_bytes_codecs(compressor)
    return Metadata3(A, chunks;
        node_type=node_type,
        fill_value=fill_value,
        order=order,
        endian=endian,
        bytes_bytes_codecs=bb_codecs,
        fill_as_missing=fill_as_missing,
        dimension_separator=chunk_key_encoding.sep
    )
end

# --- lower3: serialize MetadataV3 using codec_to_dict dispatch ---

function lower3(md::MetadataV3{T}) where T
    chunk_grid = Dict{String,Any}(
        "name" => "regular",
        "configuration" => Dict{String,Any}(
            "chunk_shape" => md.chunks |> reverse
        )
    )

    # chunk_key_encoding
    chunk_key_encoding = lower_chunk_key_encoding(md.chunk_key_encoding)

    # Build codecs from pipeline using codec_to_dict dispatch
    codecs = Dict{String,Any}[]
    p = md.pipeline

    # array->array codecs
    for codec in p.array_array
        push!(codecs, Codecs.V3Codecs.codec_to_dict(codec))
    end

    # array->bytes codec
    push!(codecs, Codecs.V3Codecs.codec_to_dict(p.array_bytes))

    # bytes->bytes codecs
    for codec in p.bytes_bytes
        push!(codecs, Codecs.V3Codecs.codec_to_dict(codec))
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

# V3 constructor from Dict - delegate to Metadata3
function Metadata(d::AbstractDict, fill_as_missing, ::ZarrFormat{3})
    return Metadata3(d, fill_as_missing)
end

function JSON.lower(md::MetadataV3)
    return lower3(md)
end
