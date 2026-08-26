import Dates: Date, DateTime
using DateTimes64: DateTime64, pydatetime_string, datetime_from_pystring 

"""NumPy array protocol type string (typestr) format

A string providing the basic type of the homogeneous array. The basic string format
consists of 3 parts: a character describing the byteorder of the data
(<: little-endian, >: big-endian, |: not-relevant), a character code giving the basic
type of the array, and an integer providing the number of bytes the type uses.

https://zarr.readthedocs.io/en/stable/spec/v2.html#data-type-encoding
"""

include("MaxLengthStrings.jl")
using .MaxLengthStrings: MaxLengthString

primitive type ASCIIChar <: AbstractChar 8 end
ASCIIChar(x::UInt8) = reinterpret(ASCIIChar, x)
ASCIIChar(x::Integer) = ASCIIChar(UInt8(x))
Base.UInt8(x::ASCIIChar) = reinterpret(UInt8, x)
Base.codepoint(x::ASCIIChar) = UInt8(x)
Base.show(io::IO, x::ASCIIChar) = print(io, Char(x))
Base.zero(::Union{ASCIIChar,Type{ASCIIChar}}) = ASCIIChar(Base.zero(UInt8))

Base.zero(t::Union{String, Type{String}}) = ""

typestr(t::Type) = string('<', 'V', sizeof(t))
typestr(t::Type{>:Missing}) = typestr(Base.nonmissingtype(t))
typestr(t::Type{Bool}) = string('|', 'b', sizeof(t))
typestr(t::Type{<:Int8}) = string("|i1")
typestr(t::Type{<:Signed}) = string('<', 'i', sizeof(t))
typestr(t::Type{<:UInt8}) = string("|u1")
typestr(t::Type{<:Unsigned}) = string('<', 'u', sizeof(t))
typestr(t::Type{Complex{T}} where T<:AbstractFloat) = string('<', 'c', sizeof(t))
typestr(t::Type{<:AbstractFloat}) = string('<', 'f', sizeof(t))
typestr(::Type{MaxLengthString{N,UInt32}}) where N = string('<', 'U', N)
typestr(::Type{MaxLengthString{N,UInt8}}) where N = string('<', 'S', N)
typestr(::Type{<:Array}) = "|O"
typestr(t::Type{<:DateTime64}) = pydatetime_string(t)
typestr(::Type{<:AbstractString}) = "|O"

const typestr_regex = r"^([<|>])([tbiufcmMOSUV])(\d*)(\[\w+\])?$"
const typemap = Dict{Tuple{Char, Int}, DataType}(
    ('b', 1) => Bool,
    ('S', 1) => ASCIIChar,
)
sizemapf(x::Type{<:Number}) = sizeof(x)
typecharf(::Type{<:Signed}) = 'i'
typecharf(::Type{<:Unsigned}) = 'u'
typecharf(::Type{<:AbstractFloat}) = 'f'
typecharf(::Type{<:Complex}) = 'c'
foreach([Float16,Float32,Float64,Int8,Int16,Int32,Int64,Int128,
  UInt8,UInt16,UInt32,UInt64,UInt128,
  Complex{Float16},Complex{Float32},Complex{Float64}]) do t
    typemap[(typecharf(t),sizemapf(t))] = t
end

function typestr(s::AbstractString, filterlist=nothing)
    m = match(typestr_regex, s)
    if m === nothing
        throw(ArgumentError("$s is not a valid numpy typestr"))
    else

        byteorder, typecode, typesize, typespec = m.captures
        if typecode == "O"
            if filterlist === nothing
                throw(ArgumentError("Object array can only be parsed when an appropriate filter is defined"))
            end
            return sourcetype(first(filterlist))
        end
        isempty(typesize) && throw((ArgumentError("$s is not a valid numpy typestr")))
        tc, ts = first(typecode), parse(Int, typesize)
        if tc == 'U'
            return MaxLengthString{ts,UInt32}
        end
        if tc == 'S' && ts > 1
            return MaxLengthString{ts,UInt8}
        end
        if tc == 'M' && ts == 8
            #We have a datetime64 value
            return datetime_from_pystring(s)
        end
        # convert typecode to Char and typesize to Int
        typemap[(tc,ts)]
    end
end


"""Metadata configuration of the stored array

Each array requires essential configuration metadata to be stored, enabling correct
interpretation of the stored data. This metadata is encoded using JSON and stored as the
value of the ".zarray" key within an array store.

# Type Parameters
* T - element type of the array
* N - dimensionality of the array
* C - compressor
* F - filters

# See Also

https://zarr.readthedocs.io/en/stable/spec/v2.html#metadata
"""
abstract type AbstractMetadata{T,N,E <: AbstractChunkKeyEncoding} end
Base.ndims(::AbstractMetadata{<:Any,N}) where N = N


"""Metadata for Zarr version 2 arrays"""
struct MetadataV2{T,N,C,F} <: AbstractMetadata{T,N,ChunkKeyEncoding}
    zarr_format::Int
    node_type::String
    shape::Base.RefValue{NTuple{N, Int}}
    chunks::NTuple{N, Int}
    dtype::String  # structured data types not yet supported
    compressor::C
    fill_value::Union{T, Nothing}
    order::Char
    filters::F  # not yet supported
    chunk_key_encoding::ChunkKeyEncoding
    function MetadataV2{T2,N,C,F}(zarr_format, node_type, shape, chunks, dtype, compressor, fill_value, order, filters, chunk_key_encoding) where {T2,N,C,F}
        zarr_format == 2 || throw(ArgumentError("MetadataV2 only functions if zarr_format == 2"))
        #Do some sanity checks to make sure we have a sane array
        any(<(0), shape) && throw(ArgumentError("Size must be positive"))
        any(<(1), chunks) && throw(ArgumentError("Chunk size must be >= 1 along each dimension"))
        order === 'C' || throw(ArgumentError("Currently only 'C' storage order is supported"))
        new{T2,N,C,F}(zarr_format, node_type, Base.RefValue{NTuple{N,Int}}(shape), chunks, dtype, compressor, fill_value, order, filters, chunk_key_encoding)
    end
end
zarr_format(::MetadataV2) = ZarrFormat(Val(2))

# Type alias for backward compatibility
const Metadata = AbstractMetadata

#To make unit tests pass with ref shape
function Base.:(==)(m1::MetadataV2, m2::MetadataV2)
  m1.zarr_format == m2.zarr_format &&
  m1.node_type == m2.node_type &&
  m1.shape[] == m2.shape[] &&
  m1.chunks == m2.chunks &&
  m1.dtype == m2.dtype &&
  m1.compressor == m2.compressor &&
  m1.fill_value == m2.fill_value &&
  m1.order == m2.order &&
  m1.filters == m2.filters &&
  m1.chunk_key_encoding == m2.chunk_key_encoding
end


"Construct Metadata based on your data"
function Metadata(A::AbstractArray{T,N}, chunks::NTuple{N,Int}, zarr_format=DV;
        node_type::String="array",
        compressor::C=default_compressor(),
        fill_value::Union{T, Nothing}=nothing,
        order::Char='C',
        filters=nothing,
        fill_as_missing = false,
        dimension_separator::Char = '.'
    ) where {T, N, C}
    return Metadata(A, chunks, ZarrFormat(zarr_format);
        node_type=node_type,
        compressor=compressor,
        fill_value=fill_value,
        order=order,
        filters=filters,
        fill_as_missing=fill_as_missing,
        chunk_key_encoding=ChunkKeyEncoding(dimension_separator, default_prefix(ZarrFormat(zarr_format)))
    )
end

# V2 constructor
function Metadata(A::AbstractArray{T,N}, chunks::NTuple{N,Int}, ::ZarrFormat{2};
        node_type::String="array",
        compressor::C=default_compressor(),
        fill_value::Union{T, Nothing}=nothing,
        order::Char='C',
        filters::F=nothing,
        fill_as_missing::Bool = false,
    chunk_key_encoding=ChunkKeyEncoding('.', false)
    ) where {T, N, C, F}
    # The two element types are spelled out in separate branches rather than
    # computed as `T2 = cond ? T : Union{T,Missing}`: a value-dependent type
    # would turn the `MetadataV2{T2,...}` constructor into a runtime
    # `Core.apply_type`, which is unresolvable for juliac `--trim` and makes
    # the return type of `zcreate` uninferable.
    if fill_value === nothing || !fill_as_missing
        MetadataV2{T,N,C,F}(
            2,
            node_type,
            size(A),
            chunks,
            typestr(eltype(A)),
            compressor,
            fill_value,
            order,
            filters,
            chunk_key_encoding,
        )
    else
        MetadataV2{Union{T,Missing},N,C,F}(
            2,
            node_type,
            size(A),
            chunks,
            typestr(eltype(A)),
            compressor,
            fill_value,
            order,
            filters,
            chunk_key_encoding,
        )
    end
end

Metadata(s::Union{AbstractString, IO}, fill_as_missing) = Metadata(JSON.parse(s; dicttype=Dict{String,Any}), fill_as_missing)

"Construct Metadata from Dict"
function Metadata(d::AbstractDict, fill_as_missing)
    zarr_format = d["zarr_format"]::Int
    zarr_format ∉ (2, 3) && throw(ArgumentError("ZarrCore.jl currently only supports v2 or v3 of the specification"))
    return Metadata(d, fill_as_missing, ZarrFormat(zarr_format))
end

# V2 constructor from Dict
function Metadata(d::AbstractDict, fill_as_missing, ::ZarrFormat{2})
    # Zarr v2 metadata is only for arrays
    node_type = "array"

    compdict = d["compressor"]
    if isnothing(compdict)
        # try the last filter, for Kerchunk compat
        if !isnothing(d["filters"]) && haskey(compressortypes, d["filters"][end]["id"])
            compdict = pop!(d["filters"]) # TODO: this will not work with JSON3!
        end
    end
    compressor = getCompressor(compdict)

    filters = getfilters(d)

    T = typestr(d["dtype"], filters)
    N = length(d["shape"])
    C = typeof(compressor)
    F = typeof(filters)

    fv = fill_value_decoding(d["fill_value"], T)

    TU = (fv === nothing || !fill_as_missing) ? T : Union{T,Missing}

    dim_sep = only(get(d, "dimension_separator", '.'))

    MetadataV2{TU,N,C,F}(
        d["zarr_format"],
        node_type,
        NTuple{N, Int}(d["shape"]) |> reverse,
        NTuple{N, Int}(d["chunks"]) |> reverse,
        d["dtype"],
        compressor,
        fv,
        first(d["order"]),
        filters,
        ChunkKeyEncoding(dim_sep, false),
    )
end


"Describes how to lower Metadata to JSON, used in json(::Metadata)"
function JSON.lower(md::MetadataV2)
    # A NamedTuple (rather than a `Dict{String,Any}`) keeps this lowering
    # statically typed: JSON serialises the fields in declaration order and
    # no dynamic `setindex!`/key-sorting is required. The emitted JSON is
    # identical up to key order (JSON.jl sorts `Dict` keys alphabetically).
    # `fill_value_encoding` returns a small `Union` (number / "NaN" / nothing);
    # funnelling it through a helper specialised on its concrete type keeps
    # every produced NamedTuple concrete.
    _lower_v2(md, fill_value_encoding(md.fill_value))
end

"""
    print_metadata(io, md, indent_json::Bool)

Serialise array metadata as JSON. The `MetadataV2` method routes through a
function barrier (`_print_metadata_v2`) that is specialised on the concrete
type of the encoded fill value, so the lowered NamedTuple - and therefore the
whole JSON writer - stays statically typed.
"""
print_metadata(io::IO, m, indent_json::Bool) =
    indent_json ? JSON.print(io, m, 4) : JSON.print(io, m)

print_metadata(io::IO, md::MetadataV2, indent_json::Bool) =
    _print_metadata_v2(io, md, fill_value_encoding(md.fill_value), indent_json)

function _print_metadata_v2(io::IO, md::MetadataV2, fill_value, indent_json::Bool)
    nt = _lower_v2(md, fill_value)
    indent_json ? JSON.print(io, nt, 4) : JSON.print(io, nt)
    nothing
end

function _lower_v2(md::MetadataV2, fill_value)
    (;
        zarr_format = Int(md.zarr_format),
        node_type = md.node_type,
        shape = md.shape[] |> reverse,
        chunks = md.chunks |> reverse,
        dtype = md.dtype,
        compressor = md.compressor,
        fill_value = fill_value,
        order = md.order,
        filters = md.filters,
        dimension_separator = md.chunk_key_encoding.sep,
    )
end


# Fill value encoding and decoding as described in
# https://zarr.readthedocs.io/en/stable/spec/v2.html#fill-value-encoding

fill_value_encoding(v) = v
fill_value_encoding(::Nothing)=nothing
function fill_value_encoding(v::AbstractFloat)
    if isnan(v)
        "NaN"
    elseif isinf(v)
        v>0 ? "Infinity" : "-Infinity"
    else
        v
    end
end

Base.eltype(::AbstractMetadata{T}) where T = T

# this correctly parses "NaN" and "Infinity"
fill_value_decoding(v::AbstractString, T::Type{<:Number}) = parse(T, v)
fill_value_decoding(v::Nothing, ::Any) = v
fill_value_decoding(v, T) = T(v)
fill_value_decoding(v::Number, T::Type{String}) = v == 0 ? "" : T(UInt8[v])
fill_value_decoding(v, ::Type{ASCIIChar}) = v == "" ? nothing : v
fill_value_decoding(v::Nothing, ::Type{ZarrCore.ASCIIChar}) = v
fill_value_decoding(v::Vector, T::Type{<:Complex}) = T(v[1], v[2])
# Sometimes when translating between CF (climate and forecast) convention data
# and Zarr groups, fill values are left as "negative integers" to encode unsigned
# integers.  So, we have to convert to the signed type with the same number of bytes
# as the unsigned integer, then reinterpret as unsigned.  That's how a fill value 
# of -1 can have a realistic meaning with an unsigned dtype.
# However, we have to apply this correction only if the integer is negative.  
# If it's positive, then the value might be out of range of the signed integer type.
fill_value_decoding(v::Integer, T::Type{<: Unsigned}) = sign(v) < 0 ? reinterpret(T, signed(T)(v)) : T(v)


# ## Statically typed `.zarray` parsing (juliac `--trim=safe` clean path)
#
# The dynamic `Metadata(::AbstractDict, ...)` path above parses `.zarray` into a
# `Dict{String,Any}` and derives `T`, `N` and the compressor type from the
# parsed values. That is inherently dynamic: the element type of the resulting
# `ZArray` is a runtime value, so juliac's `--trim` verifier cannot resolve the
# downstream chunk-decoding calls.
#
# The types below mirror the v2 `.zarray` schema exactly and with fully concrete
# field types, so `JSON.parse(bytes, ZarrayJSON)` is statically resolvable.
# `MetadataV2(T, Val(N), C, ::ZarrayJSON, fill_as_missing)` then *checks* the
# on-disk schema against the caller-supplied `T`/`N`/`C` instead of inferring
# them. See `zopen(::Type{T}, ::Val{N}, ...)`.

# `JSON.@defaults` / `JSON.@choosetype` expand to code that names `StructUtils`
# directly, so the binding has to be visible here (`ZarrCore` only `import`s JSON).
using JSON: StructUtils

"""
    FillValueJSON

A small tagged union holding the Zarr v2 `fill_value` exactly as it appears on
disk: JSON `null`, a number (kept as `Int64` when integral, `Float64`
otherwise), a string (`"NaN"` / `"Infinity"` / `"-Infinity"`, or a base64 blob
for `|Vn` dtypes) or a boolean. Decode it for a known element type with
[`fill_value_typed`](@ref).

# Why not a `Union` field with `JSON.@choosetype`

`JSON.@choosetype U f` expands to `make(style, f(source), source, tags)`, i.e.
dispatch on a *value* of type `Type` whose possible values are `U`'s members.
Inference only recovers a static call by union-splitting that phi node, which
holds for a three-member union but widens to plain `Type` at four or more - and
a widened `Type` there makes the whole `.zarray` parse an unresolved dynamic
call under juliac `--trim=safe`. Dispatching `JSON.lift` on the *parsed value*
instead is statically resolvable for any number of alternatives, and (unlike
the `@choosetype` route) preserves full `Int64` precision, since
`JSON.gettype` reports `JSONTypes.NUMBER` rather than `JSONTypes.INT`.
"""
JSON.@nonstruct struct FillValueJSON
    kind::UInt8
    float::Float64
    int::Int64
    str::String
    bool::Bool
end

const FV_NULL = 0x00
const FV_FLOAT = 0x01
const FV_INT = 0x02
const FV_STRING = 0x03
const FV_BOOL = 0x04

"The `fill_value` of an array that stores JSON `null`."
const FILL_VALUE_NULL = FillValueJSON(FV_NULL, 0.0, 0, "", false)

JSON.lift(::Type{FillValueJSON}, ::Nothing) = FILL_VALUE_NULL
JSON.lift(::Type{FillValueJSON}, x::String) = FillValueJSON(FV_STRING, 0.0, 0, x, false)
JSON.lift(::Type{FillValueJSON}, x::Float64) = FillValueJSON(FV_FLOAT, x, 0, "", false)
JSON.lift(::Type{FillValueJSON}, x::Int64) = FillValueJSON(FV_INT, 0.0, x, "", false)
JSON.lift(::Type{FillValueJSON}, x::Bool) = FillValueJSON(FV_BOOL, 0.0, 0, "", x)

# `JSON.@nonstruct` emits `StructUtils.structlike(::StructStyle, ::Type{<:FillValueJSON}) = false`,
# which `Test.detect_ambiguities` reports as ambiguous against JSON's
# `structlike(::JSONReadStyle{O,N,S}, ::Type{T}) where S<:JSONStyle`. The
# intersection is never instantiated here (ZarrCore parses with
# `StructUtils.DefaultStyle`, which is not a `JSONStyle`), but spelling out the
# more specific method keeps the ambiguity out of package-quality checks.
@static if isdefined(JSON, :JSONStyle)
    StructUtils.structlike(::JSON.JSONReadStyle{O,N,S}, ::Type{<:FillValueJSON}) where {O,N,S<:JSON.JSONStyle} = false
end

"`true` when the stored `fill_value` is JSON `null` (the array has no fill value)."
isnullfill(v::FillValueJSON) = v.kind == FV_NULL

"""
    CompressorJSON

The on-disk (numcodecs) JSON representation of a Zarr v2 compressor: the `id`
plus the union of every configuration key the compressor packages shipped with
Zarr.jl understand. Unknown keys in the document are ignored; keys that are
absent stay `nothing`.

A `getCompressor(::Type{C}, ::CompressorJSON)` method per compressor package
turns this into a concrete compressor - see [`getCompressor`](@ref). Besides the
all-positional constructor there is a keyword constructor
`CompressorJSON(id; cname, clevel, shuffle, blocksize, level, checksum)` for
building one in code, in which every configuration key defaults to `nothing`.
"""
JSON.@defaults struct CompressorJSON
    id::String = ""
    cname::Union{Nothing,String} = nothing
    clevel::Union{Nothing,Int} = nothing
    shuffle::Union{Nothing,Int} = nothing
    blocksize::Union{Nothing,Int} = nothing
    level::Union{Nothing,Int} = nothing
    checksum::Union{Nothing,Bool} = nothing
end

# Constructor for building a `CompressorJSON` in code: `id` is required, every
# configuration key is optional and defaults to `nothing`. `id` has to stay
# positional - a keyword-only method would carry the positional signature
# `CompressorJSON()` and so overwrite the zero-argument constructor
# `JSON.@defaults` emits for the parser, which is an error during precompilation.
CompressorJSON(id::AbstractString; cname=nothing, clevel=nothing, shuffle=nothing,
    blocksize=nothing, level=nothing, checksum=nothing) =
    CompressorJSON(id, cname, clevel, shuffle, blocksize, level, checksum)

"""
    ZarrayJSON

Concrete, statically typed mirror of a Zarr v2 `.zarray` document. Missing keys
fall back to the defaults given here; unknown keys (e.g. `node_type`) are
ignored. Parse one with [`parse_zarray`](@ref).
"""
JSON.@defaults struct ZarrayJSON
    zarr_format::Int = 2
    shape::Vector{Int} = Int[]
    chunks::Vector{Int} = Int[]
    dtype::String = ""
    compressor::Union{Nothing,CompressorJSON} = nothing
    fill_value::FillValueJSON = FILL_VALUE_NULL
    order::String = "C"
    # Raw, unparsed text per filter: the typed path rejects filtered arrays
    # anyway, and `JSONText` makes no assumptions about a filter's
    # configuration keys (parsing them into a fixed schema struct would fail on
    # any filter whose key types differ from the compressor schema's).
    filters::Union{Nothing,Vector{JSON.JSONText}} = nothing
    dimension_separator::Union{Nothing,String} = nothing
end

"""
    parse_zarray(bytes) -> ZarrayJSON

Parse a Zarr v2 `.zarray` document (a `String` or a byte vector) into the
concrete [`ZarrayJSON`](@ref) schema struct. Unlike `Metadata(::AbstractString, ...)`
this produces no `Any`-typed values and is `--trim=safe` clean.
"""
parse_zarray(bytes::AbstractVector{UInt8}) = JSON.parse(bytes, ZarrayJSON)
parse_zarray(s::AbstractString) = JSON.parse(s, ZarrayJSON)

# `ntuple(..., Val(N))` (rather than `NTuple{N,Int}(v) |> reverse`) keeps the
# reversal statically unrolled and avoids the iterator protocol on a `Vector`.
_revtuple(v::Vector{Int}, ::Val{N}) where {N} = ntuple(i -> @inbounds(v[N - i + 1]), Val(N))

# Both branches return a `Char`; a ternary with differently typed branches
# would widen to `Any` under `--trim`.
function _dimension_separator(ds::Union{Nothing,String})
    if ds === nothing
        return '.'
    else
        return only(ds::String)
    end
end

"""
    fill_value_typed(::Type{T}, v::FillValueJSON) -> Union{T,Nothing}

Decode an on-disk [`FillValueJSON`](@ref) for a *known* element type `T`. Each
branch dispatches statically into an existing [`fill_value_decoding`](@ref)
method, so the result type is exactly `Union{T,Nothing}`.
"""
function fill_value_typed(::Type{T}, v::FillValueJSON) where {T}
    k = v.kind
    if k == FV_NULL
        return nothing
    elseif k == FV_STRING
        # handles "NaN" / "Infinity" / "-Infinity"
        return convert(Union{T,Nothing}, fill_value_decoding(v.str, T))
    elseif k == FV_INT
        return convert(Union{T,Nothing}, fill_value_decoding(v.int, T))
    elseif k == FV_BOOL
        return convert(Union{T,Nothing}, fill_value_decoding(v.bool, T))
    else
        return convert(Union{T,Nothing}, fill_value_decoding(v.float, T))
    end
end

"""
    MetadataV2(::Type{T}, ::Val{N}, ::Type{C}, z::ZarrayJSON, fill_as_missing::Bool)

Build array metadata for a *statically known* element type `T`, dimensionality
`N` and compressor type `C` from a parsed [`ZarrayJSON`](@ref), validating the
on-disk document against them. Throws `ArgumentError` on any mismatch.

Unlike `Metadata(::AbstractDict, ...)` every type parameter of the result is
known at compile time, which is what makes the typed
[`zopen`](@ref) path juliac `--trim=safe` clean.
"""
function MetadataV2(::Type{T}, ::Val{N}, ::Type{C}, z::ZarrayJSON, fill_as_missing::Bool) where {T,N,C<:Compressor}
    z.zarr_format == 2 || throw(ArgumentError("not a Zarr v2 array (zarr_format is not 2)"))
    ts = typestr(T)
    z.dtype == ts || throw(ArgumentError(string(
        "dtype mismatch: the store holds \"", z.dtype, "\" but \"", ts, "\" was requested")))
    (length(z.shape) == N && length(z.chunks) == N) || throw(ArgumentError(string(
        "ndims mismatch: the store holds a ", string(length(z.shape)),
        "-dimensional array but ", string(N), " dimensions were requested")))
    z.filters === nothing || throw(ArgumentError(
        "filters are not supported on the statically typed open path; use the dynamic `zopen`"))
    comp = getCompressor(C, z.compressor)
    fv = fill_value_typed(T, z.fill_value)
    sep = _dimension_separator(z.dimension_separator)
    ord = only(z.order)
    sh = _revtuple(z.shape, Val(N))
    ch = _revtuple(z.chunks, Val(N))
    # Two explicit branches: `TU = cond ? T : Union{T,Missing}` would make the
    # `MetadataV2{TU,...}` constructor a runtime `Core.apply_type`.
    if isnullfill(z.fill_value) || !fill_as_missing
        return MetadataV2{T,N,C,Nothing}(
            2, "array", sh, ch, z.dtype, comp, fv, ord, nothing, ChunkKeyEncoding(sep, false))
    else
        return MetadataV2{Union{T,Missing},N,C,Nothing}(
            2, "array", sh, ch, z.dtype, comp, fv, ord, nothing, ChunkKeyEncoding(sep, false))
    end
end
