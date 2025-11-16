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
typestr(t::Type{Bool}) = string('<', 'b', sizeof(t))
typestr(t::Type{<:Signed}) = string('<', 'i', sizeof(t))
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
    ('U', 1) => Char,
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
        if byteorder == ">"
            throw(ArgumentError("Big-endian data not yet supported"))
        end
        if typecode == "O"
            if filterlist === nothing
                throw(ArgumentError("Object array can only be parsed when an appropriate filter is defined"))
            end
            return sourcetype(first(filterlist))
        end
        isempty(typesize) && throw((ArgumentError("$s is not a valid numpy typestr")))
        tc, ts = first(typecode), parse(Int, typesize)
        if (tc in ('U','S')) && ts > 1
          return MaxLengthString{ts,tc=='U' ? UInt32 : UInt8}
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
* S - dimension separator

# See Also

https://zarr.readthedocs.io/en/stable/spec/v2.html#metadata
"""
abstract type AbstractMetadata{T, N, C, F, S} end

"""Metadata for Zarr version 2 arrays"""
struct MetadataV2{T, N, C, F, S} <: AbstractMetadata{T, N, C, F, S}
    zarr_format::Int
    node_type::String
    shape::Base.RefValue{NTuple{N, Int}}
    chunks::NTuple{N, Int}
    dtype::String  # structured data types not yet supported
    compressor::C
    fill_value::Union{T, Nothing}
    order::Char
    filters::F  # not yet supported
    function MetadataV2{T2, N, C, F, S}(zarr_format, node_type, shape, chunks, dtype, compressor, fill_value, order, filters) where {T2,N,C,F,S}
        zarr_format == 2 || throw(ArgumentError("MetadataV2 only functions if zarr_format == 2"))
        #Do some sanity checks to make sure we have a sane array
        any(<(0), shape) && throw(ArgumentError("Size must be positive"))
        any(<(1), chunks) && throw(ArgumentError("Chunk size must be >= 1 along each dimension"))
        order === 'C' || throw(ArgumentError("Currently only 'C' storage order is supported"))
        new{T2, N, C, F, S}(zarr_format, node_type, Base.RefValue{NTuple{N,Int}}(shape), chunks, dtype, compressor,fill_value, order, filters)
    end
    function MetadataV2{T2, N, C, F}(
        zarr_format,
        node_type,
        shape,
        chunks,
        dtype,
        compressor,
        fill_value,
        order,
        filters,
        dimension_separator::Char = '.'
    ) where {T2,N,C,F}
        return MetadataV2{T2, N, C, F, dimension_separator}(
            zarr_format,
            node_type,
            shape,
            chunks,
            dtype,
            compressor,
            fill_value,
            order,
            filters
        )
    end
end

"""Metadata for Zarr version 3 arrays"""
struct MetadataV3{T, N, C, F, S} <: AbstractMetadata{T, N, C, F, S}
    zarr_format::Int
    node_type::String
    shape::Base.RefValue{NTuple{N, Int}}
    chunks::NTuple{N, Int}
    dtype::String  # data_type in v3
    compressor::C
    fill_value::Union{T, Nothing}
    order::Char
    filters::F  # not yet supported
    function MetadataV3{T2, N, C, F, S}(zarr_format, node_type, shape, chunks, dtype, compressor, fill_value, order, filters) where {T2,N,C,F,S}
        zarr_format == 3 || throw(ArgumentError("MetadataV3 only functions if zarr_format == 3"))
        #Do some sanity checks to make sure we have a sane array
        any(<(0), shape) && throw(ArgumentError("Size must be positive"))
        any(<(1), chunks) && throw(ArgumentError("Chunk size must be >= 1 along each dimension"))
        order === 'C' || throw(ArgumentError("Currently only 'C' storage order is supported"))
        new{T2, N, C, F, S}(zarr_format, node_type, Base.RefValue{NTuple{N,Int}}(shape), chunks, dtype, compressor,fill_value, order, filters)
    end
end

# Type alias for backward compatibility
const Metadata = AbstractMetadata

const DimensionSeparatedMetadata{S} = AbstractMetadata{<: Any, <: Any, <: Any, <: Any, S}

function Base.getproperty(m::DimensionSeparatedMetadata{S}, name::Symbol) where S
    if name == :dimension_separator
        return S
    end
    return getfield(m, name)
end
Base.propertynames(m::AbstractMetadata) = (fieldnames(typeof(m))..., :dimension_separator)

#To make unit tests pass with ref shape
import Base.==
function ==(m1::AbstractMetadata, m2::AbstractMetadata)
  m1.zarr_format == m2.zarr_format &&
  m1.node_type == m2.node_type &&
  m1.shape[] == m2.shape[] &&
  m1.chunks == m2.chunks &&
  m1.dtype == m2.dtype &&
  m1.compressor == m2.compressor &&
  m1.fill_value == m2.fill_value &&
  m1.order == m2.order &&
  m1.filters == m2.filters &&
  m1.dimension_separator == m2.dimension_separator
end


"Construct Metadata based on your data"
function Metadata(A::AbstractArray{T, N}, chunks::NTuple{N, Int};
        zarr_format::Integer=2,
        node_type::String="array",
        compressor::C=BloscCompressor(),
        fill_value::Union{T, Nothing}=nothing,
        order::Char='C',
        filters::Nothing=nothing,
        fill_as_missing = false,
        dimension_separator::Char = '.'
    ) where {T, N, C}
    T2 = (fill_value === nothing || !fill_as_missing) ? T : Union{T,Missing}
    if zarr_format == 2
        MetadataV2{T2, N, C, typeof(filters), dimension_separator}(
            zarr_format,
            node_type,
            size(A),
            chunks,
            typestr(eltype(A)),
            compressor,
            fill_value,
            order,
            filters
        )
    elseif zarr_format == 3
        @warn("Zarr v3 support is experimental")
        MetadataV3{T2, N, C, typeof(filters), dimension_separator}(
            zarr_format,
            node_type,
            size(A),
            chunks,
            typestr(eltype(A)),
            compressor,
            fill_value,
            order,
            filters
        )
    else
        throw(ArgumentError("Zarr.jl currently only supports v2 or v3 of the specification"))
    end
end

Metadata(s::Union{AbstractString, IO},fill_as_missing) = Metadata(JSON.parse(s),fill_as_missing)

"Construct Metadata from Dict"
function Metadata(d::AbstractDict, fill_as_missing)
    # create a Metadata struct from it

    if d["zarr_format"] == 3
        return Metadata3(d, fill_as_missing)
    end

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

    S = only(get(d, "dimension_separator", '.'))

    MetadataV2{TU, N, C, F, S}(
        d["zarr_format"],
        node_type,
        NTuple{N, Int}(d["shape"]) |> reverse,
        NTuple{N, Int}(d["chunks"]) |> reverse,
        d["dtype"],
        compressor,
        fv,
        first(d["order"]),
        filters,
    )
end

"Describes how to lower Metadata to JSON, used in json(::Metadata)"
function JSON.lower(md::MetadataV2)
    Dict{String, Any}(
        "zarr_format" => md.zarr_format,
        "node_type" => md.node_type,
        "shape" => md.shape[] |> reverse,
        "chunks" => md.chunks |> reverse,
        "dtype" => md.dtype,
        "compressor" => md.compressor,
        "fill_value" => fill_value_encoding(md.fill_value),
        "order" => md.order,
        "filters" => md.filters,
        "dimension_separator" => md.dimension_separator
    )
end

function JSON.lower(md::MetadataV3)
    return lower3(md)
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
# Sometimes when translating between CF (climate and forecast) convention data
# and Zarr groups, fill values are left as "negative integers" to encode unsigned
# integers.  So, we have to convert to the signed type with the same number of bytes
# as the unsigned integer, then reinterpret as unsigned.  That's how a fill value 
# of -1 can have a realistic meaning with an unsigned dtype.
# However, we have to apply this correction only if the integer is negative.  
# If it's positive, then the value might be out of range of the signed integer type.
fill_value_decoding(v::Integer, T::Type{<: Unsigned}) = sign(v) < 0 ? reinterpret(T, signed(T)(v)) : T(v)
