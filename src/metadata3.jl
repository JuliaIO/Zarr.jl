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

        group_pipeline = V3Pipeline((), Codecs.V3Codecs.BytesCodec(), ())
        return MetadataV3{Int,0,typeof(group_pipeline)}(zarr_format, node_type, (), (), "", group_pipeline, 0, 'C', ChunkEncoding('/', true))
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
    if chunk_key_encoding["name"] ∉ ("default", "v2")
        throw(ArgumentError("Unknown chunk_key_encoding of name, $(chunk_key_encoding["name"])"))
    end

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
            if haskey(config, "endian")
                config["endian"] == "little" ||
                    throw(ArgumentError("Zarr.jl currently only supports little endian for the bytes codec"))
            end
            array_bytes_codec = Codecs.V3Codecs.BytesCodec()
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

    cke_configuration = get(chunk_key_encoding, "configuration") do
        Dict{String,Any}()
    end
    # V2 uses '.' while default CKE uses '/' by default
    if chunk_key_encoding["name"] == "v2"
        separator = only(get(cke_configuration, "separator", '.'))
        chunk_encoding = ChunkEncoding(separator, false)
    elseif chunk_key_encoding["name"] == "default"
        chunk_encoding = ChunkEncoding(only(get(cke_configuration, "separator", '/')), true)
    end

    MetadataV3{TU, N, typeof(pipeline)}(
        zarr_format,
        node_type,
        NTuple{N, Int}(shape) |> reverse,
        NTuple{N, Int}(chunks) |> reverse,
        data_type,
        pipeline,
        fv,
        order,
        chunk_encoding,
    )
end

"Construct MetadataV3 based on your data"
function Metadata3(A::AbstractArray{T, N}, chunks::NTuple{N, Int};
        node_type::String="array",
        compressor=BloscCompressor(),
        fill_value::Union{T, Nothing}=nothing,
        order::Char='C',
        filters=nothing,
        fill_as_missing = false,
        dimension_separator::Char = '/'
    ) where {T, N}
    @warn("Zarr v3 support is experimental")
    T2 = (fill_value === nothing || !fill_as_missing) ? T : Union{T,Missing}
    if fill_value === nothing
        fill_value = zero(T)
    end

    # Build V3Pipeline
    array_bytes_codec = Codecs.V3Codecs.BytesCodec()
    bytes_bytes_codecs = if compressor isa NoCompressor
        ()
    elseif compressor isa BloscCompressor
        (Codecs.V3Codecs.BloscV3Codec(compressor.cname, compressor.clevel, compressor.shuffle, compressor.blocksize, sizeof(T)),)
    elseif compressor isa ZlibCompressor
        (Codecs.V3Codecs.GzipV3Codec(compressor.config.level),)
    elseif compressor isa ZstdCompressor
        (Codecs.V3Codecs.ZstdV3Codec(compressor.config.compressionLevel),)
    else
        throw(ArgumentError("Unsupported compressor type for v3: $(typeof(compressor))"))
    end
    pipeline = V3Pipeline((), array_bytes_codec, bytes_bytes_codecs)

    MetadataV3{T2,N,typeof(pipeline)}(
        3,
        node_type,
        size(A),
        chunks,
        typestr3(eltype(A)),
        pipeline,
        fill_value,
        order,
        ChunkEncoding(dimension_separator, true)
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
    chunk_key_encoding = Dict{String,Any}(
        "name" => md.chunk_encoding.prefix ? "default" : "v2",
        "configuration" => Dict{String,Any}(
            "separator" => string(md.chunk_encoding.sep)
        )
    )

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
            "configuration" => Dict{String,Any}("endian" => "little")
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
