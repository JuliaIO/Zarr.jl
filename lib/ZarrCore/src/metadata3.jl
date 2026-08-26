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

# One explicit `typestr3` method per `typemap3` entry. The generic fallback
# above computes the name with `lowercase(string(t))`, which drags the whole
# `show`/`string(::Type)` machinery into a compiled program and is not
# statically foldable; a method per type makes the dtype check of the typed v3
# open path a comparison against a constant `String` under juliac `--trim=safe`.
# The names are the wire names, so `ComplexF32` maps to `"complex64"` (not the
# fallback's `"complexf32"`) - which is what the v3 spec asks for.
for (_typestr3_name, _typestr3_type) in typemap3
    @eval typestr3(::Type{$_typestr3_type}) = $_typestr3_name
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
struct MetadataV3{T,N,P<:AbstractCodecPipeline,E<:AbstractChunkKeyEncoding} <: AbstractMetadata{T,N,E}
    zarr_format::Int
    node_type::String
    shape::Base.RefValue{NTuple{N, Int}}
    chunks::NTuple{N, Int}
    dtype::Union{String, Dict{String, Any}}  # data_type in v3
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
        dtype, fill_value;
        order::Char='C',
        endian::Symbol=:little,
        compressor=default_compressor(),
        chunk_key_encoding::E=ChunkKeyEncoding('/', true)
    ) where {T2, N, E}
    T_base = Base.nonmissingtype(T2)
    if T_base <: AbstractString && T_base !== MaxLengthString
        array_bytes_codec = Codecs.V3Codecs.VLenUTF8V3Codec()
        typesize = 4
    else
        array_bytes_codec = Codecs.V3Codecs.BytesCodec(endian)
        typesize = sizeof(T_base)
    end
    bytes_bytes_codecs = v2_to_v3_codecs(compressor, typesize)
    # The `order` branch changes the *type* of the array->array stage, and hence
    # of the pipeline. Naming `MetadataV3{T2,N,typeof(pipeline),E}` after the
    # branches joined would be a runtime `Core.apply_type`, which
    # `juliac --trim=safe` rejects; instead each branch calls the positional
    # barrier `_metadata_v3`, which gets the pipeline type as a static parameter.
    if order == 'F'
        array_array_codecs = (Codecs.V3Codecs.TransposeCodec(ntuple(i -> N - i + 1, N)),)
        return _metadata_v3(T2, Val(N),
            V3Pipeline(array_array_codecs, array_bytes_codec, bytes_bytes_codecs),
            zarr_format, node_type, shape, chunks, dtype, fill_value, chunk_key_encoding)
    else
        return _metadata_v3(T2, Val(N),
            V3Pipeline((), array_bytes_codec, bytes_bytes_codecs),
            zarr_format, node_type, shape, chunks, dtype, fill_value, chunk_key_encoding)
    end
end

"""
    _metadata_v3(::Type{T2}, ::Val{N}, pipeline::P, zarr_format, node_type, shape,
                 chunks, dtype, fill_value, cke::E)

Positional function barrier behind the `MetadataV3{T2,N}(...)` convenience
constructor. `P` and `E` arrive as static type parameters, so the
`MetadataV3{T2,N,P,E}` instantiation is a compile-time constant.
"""
function _metadata_v3(::Type{T2}, ::Val{N}, pipeline::P, zarr_format, node_type,
        shape::NTuple{N,Int}, chunks::NTuple{N,Int}, dtype, fill_value, cke::E) where {T2,N,P,E}
    return MetadataV3{T2,N,P,E}(zarr_format, node_type, shape, chunks, dtype,
        pipeline, fill_value, cke)
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

# ## Statically typed (juliac `--trim=safe`) `zarr.json` schema
#
# The mirror of `ZarrayJSON` for Zarr v3. Same rules as the v2 schema: concrete
# field types only, no `Any` reaching JSON, defaults for every field so that
# `JSON.@defaults` emits a zero-argument constructor for the parser. See
# `metadata.jl` for the rationale behind `FillValueJSON` (reused verbatim) and
# behind `JSON.JSONText` for values the typed path rejects anyway.

# Nested JSON *arrays* are kept as raw text for the same reason the nested
# objects of `ZarrJsonV3` are (see its docstring): `make(Vector{Int})` is one
# more level of the `StructUtils.make` cycle, and a cycle deeper than two is
# inferred with limited accuracy and reported as an unresolved call by juliac
# `--trim=safe`. `_json_int_vector` parses one from its own entry point.

"The `JSONText` a schema struct stores for an absent nested object."
const JSON_NULL_TEXT = JSON.JSONText("null")

"The `JSONText` a schema struct stores for an absent nested array."
const JSON_EMPTY_ARRAY_TEXT = JSON.JSONText("[]")

"""
    _json_int_vector(t::JSON.JSONText) -> Vector{Int}

Parse a raw JSON array of integers. Throws an `ArgumentError` for any other
JSON value, which is what rejects the deprecated `"C"`/`"F"` spelling of a
transpose codec's `order`.
"""
function _json_int_vector(t::JSON.JSONText)
    s = t.value
    startswith(s, "[") || throw(ArgumentError(string(
        "expected a JSON array of integers, got ", s)))
    return JSON.parse(s, Vector{Int})
end

"Serialize a vector of integers back to raw JSON text (for building schema structs in code)."
function _int_vector_json(v)
    io = IOBuffer()
    write(io, '[')
    first = true
    for x in v
        first || write(io, ',')
        first = false
        write(io, string(Int(x)))
    end
    write(io, ']')
    return JSON.JSONText(String(take!(io)))
end

"""
    CodecConfigJSON

The `"configuration"` object of a Zarr v3 codec, as a flat union of every
configuration key the codecs shipped with Zarr.jl understand. Keys that are
absent from the document stay `nothing`; unknown keys are ignored.

`endian` belongs to `bytes`, `level` to `zstd`/`gzip`, `checksum` to `zstd`,
`cname`/`clevel`/`shuffle`/`blocksize`/`typesize` to `blosc` and `order` to
`transpose`. `order` is kept as raw JSON text; decode it with
`ZarrCore._json_int_vector`, which rejects the deprecated `"C"`/`"F"` string
spelling of the transpose order with an `ArgumentError`.
"""
JSON.@defaults struct CodecConfigJSON
    endian::Union{Nothing,String} = nothing
    level::Union{Nothing,Int} = nothing
    checksum::Union{Nothing,Bool} = nothing
    cname::Union{Nothing,String} = nothing
    clevel::Union{Nothing,Int} = nothing
    shuffle::Union{Nothing,String} = nothing
    blocksize::Union{Nothing,Int} = nothing
    typesize::Union{Nothing,Int} = nothing
    # Raw text; parse with `_json_int_vector`. See the note above.
    order::Union{Nothing,JSON.JSONText} = nothing
end

"""
    CodecJSON

One entry of the `"codecs"` array of a Zarr v3 `zarr.json`: a `name` plus an
optional [`CodecConfigJSON`](@ref) configuration.

A `getCodec(::Type{C}, ::CodecJSON, ctx)` method per codec type turns this into
a concrete codec - see `ZarrCore.Codecs.V3Codecs.getCodec`. Besides the
all-positional constructor there is a keyword constructor
`CodecJSON(name; endian, level, checksum, cname, clevel, shuffle, blocksize,
typesize, order)` for building one in code.
"""
JSON.@defaults struct CodecJSON
    name::String = ""
    configuration::Union{Nothing,CodecConfigJSON} = nothing
end

# Constructor for building a `CodecJSON` in code. As with `CompressorJSON`,
# `name` has to stay positional: a keyword-only method would carry the
# positional signature `CodecJSON()` and so overwrite the zero-argument
# constructor `JSON.@defaults` emits for the parser.
function CodecJSON(name::AbstractString; endian=nothing, level=nothing, checksum=nothing,
        cname=nothing, clevel=nothing, shuffle=nothing, blocksize=nothing,
        typesize=nothing, order=nothing)
    if endian === nothing && level === nothing && checksum === nothing && cname === nothing &&
            clevel === nothing && shuffle === nothing && blocksize === nothing &&
            typesize === nothing && order === nothing
        return CodecJSON(String(name), nothing)
    end
    ordertext = order === nothing ? nothing : _int_vector_json(order)
    return CodecJSON(String(name), CodecConfigJSON(endian, level, checksum, cname, clevel,
        shuffle, blocksize, typesize, ordertext))
end

"""
The `"configuration"` object of a Zarr v3 `chunk_grid`. `chunk_shape` is raw
JSON text; decode it with `ZarrCore._json_int_vector`.
"""
JSON.@defaults struct ChunkGridConfigJSON
    chunk_shape::JSON.JSONText = JSON_EMPTY_ARRAY_TEXT
end

"""The `"chunk_grid"` object of a Zarr v3 `zarr.json`; only `"regular"` is supported."""
JSON.@defaults struct ChunkGridJSON
    name::String = ""
    configuration::Union{Nothing,ChunkGridConfigJSON} = nothing
end

"""The `"configuration"` object of a Zarr v3 `chunk_key_encoding`."""
JSON.@defaults struct ChunkKeyEncodingConfigJSON
    separator::Union{Nothing,String} = nothing
end

"""The `"chunk_key_encoding"` object of a Zarr v3 `zarr.json`."""
JSON.@defaults struct ChunkKeyEncodingJSON
    name::String = ""
    configuration::Union{Nothing,ChunkKeyEncodingConfigJSON} = nothing
end

"""
    ZarrJsonV3

Concrete, statically typed mirror of a Zarr v3 `zarr.json` array document.
Missing keys fall back to the defaults given here; unknown keys (`attributes`,
`dimension_names`, ...) are ignored - attributes are read separately by
[`getattrs_typed`](@ref). Parse one with [`parse_zarrjson`](@ref).

`data_type` is typed `String`, so the object spelling of a data type
(`{"name": "fixed_length_utf32", ...}`) fails at parse time; those arrays have
to be opened with the dynamic `zopen`.

# Why the nested objects are `JSONText`

`chunk_grid`, `chunk_key_encoding` and the entries of `codecs` are kept as raw
text and parsed on demand by [`chunk_grid_json`](@ref),
[`chunk_key_encoding_json`](@ref) and [`codecs_json`](@ref). Parsing them in
place would nest `StructUtils.make` three deep (document -> object ->
`configuration`), and `make` recursing into itself that far is inferred only
with limited accuracy, which juliac `--trim=safe` reports as an unresolved
call. Parsing each nested object from its own top-level entry point keeps every
`make` cycle exactly as deep as the v2 [`ZarrayJSON`](@ref) one, which is known
to be trim-clean.
"""
JSON.@defaults struct ZarrJsonV3
    zarr_format::Int = 3
    node_type::String = ""
    shape::Vector{Int} = Int[]
    data_type::String = ""
    chunk_grid::JSON.JSONText = JSON_NULL_TEXT
    chunk_key_encoding::JSON.JSONText = JSON_NULL_TEXT
    fill_value::FillValueJSON = FILL_VALUE_NULL
    codecs::Vector{JSON.JSONText} = JSON.JSONText[]
    # Raw, unparsed text per storage transformer: the typed path rejects any
    # non-empty list anyway, and `JSONText` makes no assumptions about the keys.
    storage_transformers::Union{Nothing,Vector{JSON.JSONText}} = nothing
end

"""
    parse_zarrjson(bytes) -> ZarrJsonV3

Parse a Zarr v3 `zarr.json` document (a `String` or a byte vector) into the
concrete [`ZarrJsonV3`](@ref) schema struct. Unlike `Metadata3(::AbstractDict, ...)`
this produces no `Any`-typed values and is `--trim=safe` clean.
"""
parse_zarrjson(bytes::AbstractVector{UInt8}) = JSON.parse(bytes, ZarrJsonV3)
parse_zarrjson(s::AbstractString) = JSON.parse(s, ZarrJsonV3)

"""
    chunk_grid_json(z::ZarrJsonV3) -> ChunkGridJSON

Parse the `"chunk_grid"` object of a parsed [`ZarrJsonV3`](@ref).
"""
function chunk_grid_json(z::ZarrJsonV3)
    t = z.chunk_grid.value
    t == "null" && throw(ArgumentError("the zarr.json document has no chunk_grid"))
    return JSON.parse(t, ChunkGridJSON)
end

"""
    chunk_key_encoding_json(z::ZarrJsonV3) -> ChunkKeyEncodingJSON

Parse the `"chunk_key_encoding"` object of a parsed [`ZarrJsonV3`](@ref).
"""
function chunk_key_encoding_json(z::ZarrJsonV3)
    t = z.chunk_key_encoding.value
    t == "null" && throw(ArgumentError("the zarr.json document has no chunk_key_encoding"))
    return JSON.parse(t, ChunkKeyEncodingJSON)
end

"""
    codecs_json(z::ZarrJsonV3) -> Vector{CodecJSON}

Parse the `"codecs"` array of a parsed [`ZarrJsonV3`](@ref) into the concrete
[`CodecJSON`](@ref) schema. The result is what
`ZarrCore.Codecs.V3Codecs.pipeline_from_json` consumes.
"""
function codecs_json(z::ZarrJsonV3)
    texts = z.codecs
    n = length(texts)
    out = Vector{CodecJSON}(undef, n)
    for i in 1:n
        out[i] = JSON.parse(texts[i].value, CodecJSON)
    end
    return out
end

"""
    _chunk_key_encoding_typed(c::ChunkKeyEncodingJSON) -> ChunkKeyEncoding

Typed counterpart of [`parse_chunk_key_encoding`](@ref) covering the two
encodings whose result type is exactly `ChunkKeyEncoding`: `"default"`
(prefixed, default separator `'/'`) and `"v2"` (unprefixed, default separator
`'.'`). `"suffix"` (and any unregistered name) throws an `ArgumentError`.
"""
function _chunk_key_encoding_typed(c::ChunkKeyEncodingJSON)
    sep_str = ""
    cfg = c.configuration
    if cfg !== nothing
        s = (cfg::ChunkKeyEncodingConfigJSON).separator
        if s !== nothing
            sep_str = s::String
        end
    end
    if c.name == "default"
        return ChunkKeyEncoding(isempty(sep_str) ? DS3 : only(sep_str), true)
    elseif c.name == "v2"
        return ChunkKeyEncoding(isempty(sep_str) ? DS2 : only(sep_str), false)
    else
        throw(ArgumentError(string("chunk_key_encoding \"", c.name,
            "\" is not supported on the statically typed open path; use the dynamic `zopen`")))
    end
end

"""
    MetadataV3(::Type{T}, ::Val{N}, ::Type{P}, z::ZarrJsonV3, fill_as_missing::Bool)

Build array metadata for a *statically known* element type `T`, dimensionality
`N` and codec pipeline type `P` from a parsed [`ZarrJsonV3`](@ref), validating
the on-disk document against them. Throws `ArgumentError` on any mismatch.

This is the v3 counterpart of `MetadataV2(::Type{T}, ::Val{N}, ::Type{C}, ::ZarrayJSON, ::Bool)`:
every type parameter of the result is known at compile time, which is what
makes the typed [`zopen`](@ref) path juliac `--trim=safe` clean.
"""
function MetadataV3(::Type{T}, ::Val{N}, ::Type{P}, z::ZarrJsonV3,
                    fill_as_missing::Bool) where {T,N,P<:V3Pipeline}
    z.zarr_format == 3 || throw(ArgumentError("not a Zarr v3 array (zarr_format is not 3)"))
    z.node_type == "array" || throw(ArgumentError(string(
        "not a Zarr v3 array: node_type is \"", z.node_type, "\"")))
    ts = typestr3(T)
    z.data_type == ts || throw(ArgumentError(string(
        "data_type mismatch: the store holds \"", z.data_type, "\" but \"", ts, "\" was requested")))
    length(z.shape) == N || throw(ArgumentError(string(
        "ndims mismatch: the store holds a ", string(length(z.shape)),
        "-dimensional array but ", string(N), " dimensions were requested")))
    grid = chunk_grid_json(z)
    grid.name == "regular" || throw(ArgumentError(string(
        "chunk_grid \"", grid.name, "\" is not supported; only \"regular\" is")))
    gcfg = grid.configuration
    gcfg === nothing && throw(ArgumentError("the chunk_grid has no configuration"))
    chunk_shape = _json_int_vector((gcfg::ChunkGridConfigJSON).chunk_shape)
    length(chunk_shape) == N || throw(ArgumentError(string(
        "chunk grid rank ", string(length(chunk_shape)),
        " does not match the array rank ", string(N))))
    st = z.storage_transformers
    if st !== nothing
        isempty(st::Vector{JSON.JSONText}) || throw(ArgumentError(
            "storage_transformers are not supported on the statically typed open path; use the dynamic `zopen`"))
    end
    isnullfill(z.fill_value) && throw(ArgumentError(
        "the stored fill_value is null, which a Zarr v3 array must not be"))
    enc = _chunk_key_encoding_typed(chunk_key_encoding_json(z))
    pipeline = Codecs.V3Codecs.pipeline_from_json(P, codecs_json(z),
        (shape = z.shape, elsize = _sizeof(T)))
    fv = fill_value_typed(T, z.fill_value)
    sh = _revtuple(z.shape, Val(N))
    ch = _revtuple(chunk_shape, Val(N))
    # Two explicit branches, as in `MetadataV2`: `TU = cond ? T : Union{T,Missing}`
    # would make the `MetadataV3{TU,...}` constructor a runtime `Core.apply_type`.
    if !fill_as_missing
        return MetadataV3{T,N,P,ChunkKeyEncoding}(
            3, "array", sh, ch, z.data_type, pipeline, fv, enc)
    else
        return MetadataV3{Union{T,Missing},N,P,ChunkKeyEncoding}(
            3, "array", sh, ch, z.data_type, pipeline, fv, enc)
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
        chunks = Int.(chunk_grid["configuration"]["chunk_shape"])
        if length(shape) != length(chunks)
            throw(ArgumentError("Shape has rank $(length(shape)) which does not match the chunk_shape rank of $(length(chunks))"))
        end
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
        NTuple{N, Int}(chunks) |> reverse,
        typestr3(T),
        pipeline,
        fv,
        chunk_key_encoding,
    )
end

"Construct MetadataV3 based on your data"
function Metadata3(A::AbstractArray{T, N}, chunks::NTuple{N, Int};
        node_type::String="array",
        compressor=default_compressor(),
        fill_value::Union{T, Nothing}=nothing,
        order::Char='C',
        endian::Symbol=:little,
        filters=nothing,
        fill_as_missing = false,
        dimension_separator::Char = '/'
    ) where {T, N}
    # `fv` is spelled with an explicit type annotation rather than reassigning
    # `fill_value`, so it stays concretely typed for the constructor.
    local fv::T
    if fill_value === nothing
        fv = zero(T)
    else
        fv = fill_value
    end
    cke = ChunkKeyEncoding(dimension_separator, true)
    # The two element types are spelled out in separate branches rather than
    # computed as `T2 = cond ? T : Union{T,Missing}`: a value-dependent type
    # would turn the `MetadataV3{T2,N}` constructor into a runtime
    # `Core.apply_type`, which is unresolvable for juliac `--trim` and makes
    # the return type of `zcreate` uninferable. Mirrors
    # `Metadata(A, chunks, ::ZarrFormat{2})` in `metadata.jl`.
    if fill_value === nothing || !fill_as_missing
        return MetadataV3{T, N}(
            3, node_type, size(A), chunks, typestr3(T), fv;
            order=order, endian=endian, compressor=compressor, chunk_key_encoding=cke)
    else
        return MetadataV3{Union{T,Missing}, N}(
            3, node_type, size(A), chunks, typestr3(T), fv;
            order=order, endian=endian, compressor=compressor, chunk_key_encoding=cke)
    end
end

"""
    lower3(md::MetadataV3) -> NamedTuple

Lower v3 array metadata into the `zarr.json` document shape. A NamedTuple (not
a `Dict{String,Any}`) so that the whole JSON writer stays statically typed under
`juliac --trim=safe`; the emitted JSON is identical up to key order (JSON.jl
sorts `Dict` keys alphabetically, a NamedTuple keeps declaration order).

`fill_value_encoding` returns a small `Union` (number / `"NaN"` / `nothing`), so
- exactly as for `MetadataV2` - the NamedTuple is built inside `_lower_v3`,
a barrier specialised on the concrete type of the encoded fill value.
"""
lower3(md::MetadataV3) = _lower_v3(md, fill_value_encoding(md.fill_value),
    Codecs.V3Codecs._pipeline_to_codec_list(md.pipeline))

function _lower_v3(md::MetadataV3{T}, fill_value, codecs) where T
    return (;
        zarr_format = Int(md.zarr_format),
        node_type = md.node_type,
        shape = md.shape[] |> reverse,
        data_type = typestr3(T),
        chunk_grid = (; name = "regular", configuration = (; chunk_shape = md.chunks |> reverse)),
        chunk_key_encoding = lower_chunk_key_encoding(md.chunk_key_encoding),
        fill_value = fill_value,
        codecs = codecs,
    )
end

function _lower_v3(md::MetadataV3{T}, fill_value, codecs, attrs) where T
    return (;
        zarr_format = Int(md.zarr_format),
        node_type = md.node_type,
        shape = md.shape[] |> reverse,
        data_type = typestr3(T),
        chunk_grid = (; name = "regular", configuration = (; chunk_shape = md.chunks |> reverse)),
        chunk_key_encoding = lower_chunk_key_encoding(md.chunk_key_encoding),
        fill_value = fill_value,
        codecs = codecs,
        attributes = attrs,
    )
end

function Metadata(A::AbstractArray{T,N}, chunks::NTuple{N,Int}, ::ZarrFormat{3};
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

# Serialise `zarr.json` through the same function barrier as `lower3`, so that
# the NamedTuple handed to the JSON writer - and therefore the writer itself -
# is specialised on the concrete type of the encoded fill value. Mirrors
# `_print_metadata_v2` in `metadata.jl`.
print_metadata(io::IO, md::MetadataV3, indent_json::Bool) =
    _print_metadata_v3(io, md, fill_value_encoding(md.fill_value), indent_json)

function _print_metadata_v3(io::IO, md::MetadataV3, fill_value, indent_json::Bool)
    nt = _lower_v3(md, fill_value, Codecs.V3Codecs._pipeline_to_codec_json(md.pipeline))
    if indent_json
        JSON.print(io, nt, 4)
    else
        JSON.print(io, nt)
    end
    nothing
end

"""Raw `{}` fragment, written verbatim by JSON.jl - see `JSON.JSONText`."""
const _EMPTY_JSON_OBJECT = JSON.JSONText("{}")

"""
    _print_metadata_v3(io, md, fill_value, attrs, indent_json)

Write the `zarr.json` document for `md` with its `attributes` object included.
`zcreate` reaches this through `write_new_metadata(::ZarrFormat{3}, s, p, m, attrs)`
so that the attributes are part of the *initial* write, and the dynamic
read-modify-write in `writeattrs(::ZarrFormat{3}, ...)` - which parses the
freshly written document back into a `Dict{String,Any}` - never runs on the
create path. That method stays for later attribute updates.
"""
function _print_metadata_v3(io::IO, md::MetadataV3, fill_value, attrs, indent_json::Bool)
    nt = _lower_v3(md, fill_value, Codecs.V3Codecs._pipeline_to_codec_json(md.pipeline), attrs)
    if indent_json
        JSON.print(io, nt, 4)
    else
        JSON.print(io, nt)
    end
    nothing
end
