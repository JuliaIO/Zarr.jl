"""
Prototype Zarr version 3 support
"""

const typemap3 = Dict{String, DataType}()
foreach([Bool, Int8, Int16, Int32, Int64, UInt8, UInt16, UInt32, UInt64, Float16, Float32, Float64]) do t
    typemap3[lowercase(string(t))] = t
end
typemap3["complex64"] = ComplexF32
typemap3["complex128"] = ComplexF64
typemap3["string"] = String

function typestr3(t::Type)
    return lowercase(string(t))
end

function typestr3(::Type{MaxLengthString{N, UInt32}}) where {N}
    return Dict{String, Any}(
        "name" => "fixed_length_utf32",
        "configuration" => Dict{String, Any}("length_bytes" => N * 4)
        )
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

typestr3(d::AbstractDict) = parse_datatype3(d)
function parse_datatype3(d)
    name = get(d, "name", nothing)

    if name == "fixed_length_utf32"
        return MaxLengthString{d["configuration"]["length_bytes"] ÷ 4, UInt32}
    end
    throw(ArgumentError("Unsupported Zarr v3 data_type: $d"))
end

function check_keys(d::AbstractDict, keys)
    for key in keys
        if !haskey(d, key)
            throw(ArgumentError("Zarr v3 metadata must have a key called $key"))
        end
    end
end

"""Metadata for Zarr version 3 arrays"""
struct MetadataV3{T,N,P<:AbstractCodecPipeline,E<:AbstractChunkKeyEncoding,CT} <: AbstractMetadata{T,N,E}
    zarr_format::Int
    node_type::String
    shape::Base.RefValue{NTuple{N, Int}}
    chunks::CT
    dtype::Union{String, Dict{String, Any}}  # data_type in v3
    pipeline::P
    fill_value::Union{T, Nothing}
    chunk_key_encoding::E
    function MetadataV3{T2,N,P,E,CT}(zarr_format, node_type, shape, chunks::CT, dtype, pipeline, fill_value, chunk_key_encoding) where {T2,N,P,E,CT}
        zarr_format == 3 || throw(ArgumentError("MetadataV3 only functions if zarr_format == 3"))
        #Do some sanity checks to make sure we have a sane array
        any(<(0), shape) && throw(ArgumentError("Size must be positive"))
        validate_v3_chunks(shape, chunks)
        new{T2,N,P,E,CT}(zarr_format, node_type, Base.RefValue{NTuple{N,Int}}(shape), chunks, dtype, pipeline, fill_value, chunk_key_encoding)
    end
end
MetadataV3{T2,N,P,E}(zarr_format, node_type, shape, chunks, dtype, pipeline, fill_value, chunk_key_encoding) where {T2,N,P,E} =
    MetadataV3{T2,N,P,E,typeof(chunks)}(zarr_format, node_type, shape, chunks, dtype, pipeline, fill_value, chunk_key_encoding)
MetadataV3{T2,N,P}(args...) where {T2,N,P} = MetadataV3{T2,N,P,ChunkKeyEncoding}(args...)
zarr_format(::MetadataV3) = ZarrFormat(Val(3))

function validate_v3_chunks(shape::NTuple{N,Int}, chunks::NTuple{N,Int}) where {N}
    any(<(1), chunks) && throw(ArgumentError("Chunk size must be >= 1 along each dimension"))
    return chunks
end

validate_v3_chunks(shape::NTuple{N,Int}, chunks::DiskArrays.GridChunks{N}) where {N} =
    validate_chunk_grid(shape, chunks)

function canonical_v3_chunks(shape::NTuple{N,Int}, chunks::NTuple{N,Int}) where {N}
    validate_v3_chunks(shape, chunks)
    return chunks
end

function canonical_v3_chunks(shape::NTuple{N,Int}, chunks::DiskArrays.GridChunks{N}) where {N}
    validate_v3_chunks(shape, chunks)
    if all(c -> c isa DiskArrays.RegularChunks, chunks.chunks)
        return map(c -> c.chunksize, chunks.chunks)
    end
    return chunks
end

"""
Convenience constructor for MetadataV3 that builds the codec pipeline from
`order` (translated to a TransposeCodec), `endian` (translated to a BytesCodec),
and `compressor` (translated to bytes->bytes codecs).
"""
function MetadataV3{T2,N}(zarr_format, node_type, shape::NTuple{N,Int}, chunks,
        dtype, fill_value;
        order::Char='C',
        endian::Symbol=:little,
        compressor=default_compressor(),
        chunk_key_encoding::E=ChunkKeyEncoding('/', true)
    ) where {T2, N, E}
    chunks = canonical_v3_chunks(shape, chunks)
    T_base = Base.nonmissingtype(T2)
    array_array_codecs = if order == 'F'
        (Codecs.V3Codecs.TransposeCodec(ntuple(i -> N - i + 1, N)),)
    else
        ()
    end
    if T_base <: AbstractString && T_base !== MaxLengthString
        array_bytes_codec = Codecs.V3Codecs.VLenUTF8V3Codec()
        typesize = 4
    else
        array_bytes_codec = Codecs.V3Codecs.BytesCodec(endian)
        typesize = sizeof(T_base)
    end
    bytes_bytes_codecs = v2_to_v3_codecs(compressor, typesize)
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

_sizeof(x) = sizeof(x)
_sizeof(x::Type{String}) = 1

function decode_rectilinear_axis(spec, axis_length::Int, axis::Int)
    if spec isa Integer
        spec >= 1 || throw(ArgumentError("Rectilinear chunk size on axis $axis must be >= 1"))
        return DiskArrays.RegularChunks(spec, 0, axis_length)
    end
    spec isa AbstractVector ||
        throw(ArgumentError("Rectilinear chunk_shapes entry for axis $axis must be an integer or a list"))

    chunk_sizes = Int[]
    for entry in spec
        if entry isa Integer
            entry >= 1 || throw(ArgumentError("Rectilinear chunk sizes must be >= 1 (axis $axis)"))
            push!(chunk_sizes, entry)
        elseif entry isa AbstractVector && length(entry) == 2 &&
                entry[1] isa Integer && entry[2] isa Integer
            value, count = entry
            value >= 1 || throw(ArgumentError("Rectilinear chunk sizes must be >= 1 (axis $axis)"))
            count >= 1 || throw(ArgumentError("Rectilinear run lengths must be >= 1 (axis $axis)"))
            append!(chunk_sizes, fill(Int(value), Int(count)))
        else
            throw(ArgumentError(
                "Invalid rectilinear chunk descriptor $entry on axis $axis; " *
                "expected an integer or [chunk_size, count]"
            ))
        end
    end

    if axis_length == 0
        isempty(chunk_sizes) || throw(ArgumentError("A zero-length axis cannot contain rectilinear chunks (axis $axis)"))
        return DiskArrays.IrregularChunks([0])
    end
    isempty(chunk_sizes) && throw(ArgumentError("Rectilinear chunk list on axis $axis cannot be empty"))

    covered = sum(chunk_sizes)
    covered >= axis_length || throw(DimensionMismatch(
        "Rectilinear chunks on axis $axis cover $covered elements, expected at least $axis_length"
    ))
    covered_before_last = covered - last(chunk_sizes)
    covered_before_last < axis_length || throw(DimensionMismatch(
        "Rectilinear chunks on axis $axis contain chunks beyond the array extent $axis_length"
    ))
    chunk_sizes[end] = axis_length - covered_before_last
    return DiskArrays.IrregularChunks(; chunksizes=chunk_sizes)
end

function encode_rectilinear_axis(chunks::DiskArrays.RegularChunks)
    chunks.offset == 0 || throw(ArgumentError("Zarr rectilinear grids cannot encode non-zero chunk offsets"))
    return chunks.chunksize
end

function encode_rectilinear_axis(chunks::DiskArrays.IrregularChunks)
    sizes = diff(chunks.offsets)
    encoded = Any[]
    i = firstindex(sizes)
    while i <= lastindex(sizes)
        j = i
        while j < lastindex(sizes) && sizes[j + 1] == sizes[i]
            j += 1
        end
        count = j - i + 1
        push!(encoded, count == 1 ? sizes[i] : Any[sizes[i], count])
        i = j + 1
    end
    return encoded
end


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
            if key ∉ ("zarr_format", "node_type", "attributes", "consolidated_metadata")
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
    data_type = d["data_type"]

    # Chunk Grid
    chunk_grid = d["chunk_grid"]
    if chunk_grid["name"] == "regular"
        chunk_shape = Int.(chunk_grid["configuration"]["chunk_shape"])
        if length(shape) != length(chunk_shape)
            throw(ArgumentError("Shape has rank $(length(shape)) which does not match the chunk_shape rank of $(length(chunk_shape))"))
        end
        chunks = NTuple{length(chunk_shape),Int}(reverse(chunk_shape))
    elseif chunk_grid["name"] == "rectilinear"
        configuration = chunk_grid["configuration"]
        get(configuration, "kind", nothing) == "inline" ||
            throw(ArgumentError("Only rectilinear chunk grids of kind \"inline\" are supported"))
        chunk_shapes = get(configuration, "chunk_shapes", nothing)
        chunk_shapes isa AbstractVector ||
            throw(ArgumentError("Rectilinear chunk grid configuration must contain chunk_shapes"))
        length(chunk_shapes) == length(shape) || throw(DimensionMismatch(
            "Shape has rank $(length(shape)) which does not match the chunk_shapes rank of $(length(chunk_shapes))"
        ))
        axes_c = map(decode_rectilinear_axis, chunk_shapes, shape, eachindex(shape))
        chunks = DiskArrays.GridChunks(reverse(axes_c)...)
    else
        throw(ArgumentError("Unknown chunk_grid of name, $(chunk_grid["name"])"))
    end

    # Chunk Key Encoding
    chunk_key_encoding = d["chunk_key_encoding"]

    # Type Parameters (computed before codec parsing so elsize is available as context)
    T = typestr3(data_type)
    N = length(shape)

    codec_ctx = (shape = shape, elsize = _sizeof(Base.nonmissingtype(T)))
    pipeline = Codecs.V3Codecs.getCodec(d["codecs"], codec_ctx)

    fv = fill_value_decoding(d["fill_value"], T)::T

    TU = (fv === nothing || !fill_as_missing) ? T : Union{T,Missing}

    chunk_key_encoding = parse_chunk_key_encoding(chunk_key_encoding)
    E = typeof(chunk_key_encoding)

    MetadataV3{TU, N, typeof(pipeline), E}(
        zarr_format,
        node_type,
        NTuple{N, Int}(shape) |> reverse,
        chunks,
        typestr3(T),
        pipeline,
        fv,
        chunk_key_encoding,
    )
end

"Construct MetadataV3 based on your data"
function Metadata3(A::AbstractArray{T, N}, chunks;
        node_type::String="array",
        compressor=default_compressor(),
        fill_value::Union{T, Nothing}=nothing,
        order::Char='C',
        endian::Symbol=:little,
        filters=nothing,
        fill_as_missing = false,
        dimension_separator::Char = '/'
    ) where {T, N}
    T2 = (fill_value === nothing || !fill_as_missing) ? T : Union{T,Missing}
    if fill_value === nothing
        fill_value = zero(T)
    end
    chunks = canonical_v3_chunks(size(A), chunks)
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
    chunk_grid = if md.chunks isa DiskArrays.GridChunks
        Dict{String,Any}(
            "name" => "rectilinear",
            "configuration" => Dict{String,Any}(
                "kind" => "inline",
                "chunk_shapes" => map(encode_rectilinear_axis, reverse(md.chunks.chunks))
            )
        )
    else
        Dict{String,Any}(
            "name" => "regular",
            "configuration" => Dict{String,Any}(
                "chunk_shape" => reverse(md.chunks)
            )
        )
    end

    # chunk_key_encoding
    chunk_key_encoding = lower_chunk_key_encoding(md.chunk_key_encoding)

    codecs = Codecs.V3Codecs._pipeline_to_codec_list(md.pipeline)

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

function Metadata(A::AbstractArray{T,N}, chunks::Union{NTuple{N,Int},DiskArrays.GridChunks{N}}, ::ZarrFormat{3};
        node_type::String="array",
        compressor::C=default_compressor(),
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
