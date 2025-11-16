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
                raise(ArgumentError("$s is not a known type"))
            end
            if mod(num_bits, 8) == 0
                return NTuple{num_bits÷8,UInt8}
            else
                raise(ArgumentError("$s must describe a raw type with bit size that is a multiple of 8 bits"))
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

        return MetadataV3{Int,0,Nothing,Nothing,'/'}(zarr_format, node_type, (), (), "", nothing, 0, 'C', nothing)
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
    if chunk_key_encoding["name"] == "default"
    elseif chunk_key_encoding["name"] == "v2"
    else
        throw(ArgumentError("Unknown chunk_key_encoding of name, $(chunk_key_encoding["name"])"))
    end


    # Codecs
    compdict = nothing

    # For transpose codec permutation tracking
    default_dim_perm = Tuple(1:length(shape))
    dim_perm = default_dim_perm

    codec_data_type = Ref(:array)

    function check_codec_data_type(codec_name, from, to)
        codec_data_type[] == from ||
            throw(ArgumentError("$codec_name found by codec_data_type is $(codec_data_type[])"))
        codec_data_type[] = to
        return nothing
    end

    for codec in d["codecs"]
        codec_name = codec["name"]
        if codec_name == "bytes"
            # array -> bytes
            check_codec_data_type(codec_name, :array, :bytes)
            if haskey(codec, "configuration")
                codec["configuration"]["endian"] == "little" ||
                    throw(ArgumentError("Zarr.jl currently only supports little endian for the bytes codec"))
            end
        elseif codec_name == "zstd"
            # bytes -> bytes
            check_codec_data_type(codec_name, :bytes, :bytes)
            compdict = codec
        elseif codec_name == "blosc"
            # bytes -> bytes
            check_codec_data_type(codec_name, :bytes, :bytes)
            compdict = codec
        elseif codec_name == "gzip"
            # bytes -> bytes
            check_codec_data_type(codec_name, :bytes, :bytes)
            compdict = codec
        elseif codec_name == "transpose"
            # array -> array
            check_codec_data_type(codec_name, :array, :array)
            _dim_order = codec["configuration"]["order"]
            if _dim_order == "C"
                @warn "Transpose codec dimension order of $_dim_order is deprecated"
                _dim_order = 1:length(shape)
            elseif _dim_order == "F"
                @warn "Transpose codec dimension order of $_dim_order is deprecated"
                _dim_order = reverse(1:length(shape))
            else
                _dim_order = Int.(codec["configuration"]["order"]) .+ 1
            end
            dim_perm = dim_perm[_dim_order]
        elseif codec_name == "sharding_indexed"
            # array -> bytes
            check_codec_data_type(codec_name, :array, :bytes)
            throw(ArgumentError("Zarr.jl currently does not support the $(codec["name"]) codec"))
        elseif codec_name == "crc32c"
            # bytes -> bytes
            check_codec_data_type(codec_name, :bytes, :bytes)
            throw(ArgumentError("Zarr.jl currently does not support the $(codec["name"]) codec"))
        else
            throw(ArgumentError("Zarr.jl currently does not support the $(codec["name"]) codec"))
        end
    end

    if dim_perm == default_dim_perm
        order = 'C'
    elseif dim_perm == reverse(default_dim_perm)
        order = 'F'
    else
        throw(ArgumentError("Dimension permutation of $dim_perm is not implemented"))
    end

    compressor = getCompressor(compdict)

    # Filters (NOT IMPLEMENTED)
    # For v3, filters are not yet implemented, so we return nothing
    filters = nothing

    # Type Parameters
    T = typestr3(data_type)
    N = length(shape)
    C = typeof(compressor)
    F = typeof(filters)

    fv = fill_value_decoding(d["fill_value"], T)::T

    TU = (fv === nothing || !fill_as_missing) ? T : Union{T,Missing}

    cke_configuration = get(chunk_key_encoding, "configuration") do
        Dict{String,Any}()
    end
    # V2 uses '.' while default CKE uses '/' by default
    if chunk_key_encoding["name"] == "v2"
        separator = only(get(cke_configuration, "separator", '.'))
        S = V2ChunkKeyEncoding{separator}()
    elseif chunk_key_encoding["name"] == "default"
        S = only(get(cke_configuration, "separator", '/'))
    end

    MetadataV3{TU, N, C, F, S}(
        zarr_format,
        node_type,
        NTuple{N, Int}(shape) |> reverse,
        NTuple{N, Int}(chunks) |> reverse,
        data_type,
        compressor,
        fv,
        order,
        filters,
    )
end

"Construct MetadataV3 based on your data"
function Metadata3(A::AbstractArray{T, N}, chunks::NTuple{N, Int};
        node_type::String="array",
        compressor::C=BloscCompressor(),
        fill_value::Union{T, Nothing}=nothing,
        order::Char='C',
        filters::F=nothing,
        fill_as_missing = false,
        dimension_separator::Char = '/'
    ) where {T, N, C, F}
    @warn("Zarr v3 support is experimental")
    T2 = (fill_value === nothing || !fill_as_missing) ? T : Union{T,Missing}
    if fill_value === nothing
        fill_value = zero(T)
    end
    MetadataV3{T2, N, C, typeof(filters), dimension_separator}(
        3,
        node_type,
        size(A),
        chunks,
        typestr3(eltype(A)),
        compressor,
        fill_value,
        order,
        filters
    )
end

function lower3(md::MetadataV3{T}) where T

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

    chunk_grid = Dict{String,Any}(
        "name" => "regular",
        "configuration" => Dict{String,Any}(
            "chunk_shape" => md.chunks |> reverse
        )
    )

    chunk_key_encoding = Dict{String,Any}(
        "name" => isa(md.dimension_separator, Char) ? "default" :
                  isa(md.dimension_separator, V2ChunkKeyEncoding) ? "v2" :
                  error("Unknown encoding for $(md.dimension_separator)"),
        "configuration" => Dict{String,Any}(
            "separator" => separator(md.dimension_separator)
        )
    )

    # TODO: Incorporate filters
    codecs = Dict{String,Any}[]

    default_dim_perm = Tuple(0:length(md.shape[])-1)

    # Encode the order as a single transpose codec (array to array)
    push!(codecs,
        Dict{String,Any}(
            "name" => "transpose",
            "configuration" => Dict(
                "order" => md.order == 'C' ? default_dim_perm :
                           md.order == 'F' ? reverse(default_dim_perm) :
                           error("Unable to encode order $(md.order)")
            )
        )
    )

    # Convert from array to bytes
    push!(codecs,
        Dict{String,Any}(
            "name" => "bytes",
            "configuration" => Dict{String, Any}(
                "endian" => "little"
            )
        )
    )
    # Compress bytes to bytes (only if not NoCompressor)
    if !(md.compressor isa NoCompressor)
        push!(codecs, JSON.lower(Compressor_v3(md.compressor)))
    end

    Dict{String, Any}(
        "zarr_format" => md.zarr_format,
        "node_type" => md.node_type,
        "shape" => md.shape[] |> reverse,
        "data_type" => typestr3(T),
        "chunk_grid" => chunk_grid,
        "chunk_key_encoding" => chunk_key_encoding,
        "fill_value" => fill_value_encoding(md.fill_value)::T,
        "codecs" => codecs
    )
end
