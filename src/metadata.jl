import Dates: Date, DateTime

"""NumPy array protocol type string (typestr) format

A string providing the basic type of the homogenous array. The basic string format
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
UInt8(x::ASCIIChar) = reinterpret(UInt8, x)
Base.codepoint(x::ASCIIChar) = UInt8(x)
Base.show(io::IO, x::ASCIIChar) = print(io, Char(x))
Base.zero(::Union{ASCIIChar,Type{ASCIIChar}}) = ASCIIChar(Base.zero(UInt8))


using Dates: Period, TimeType, Date, DateTime, Dates
import Base.==
struct DateTime64{P} <: TimeType
    i::Int64
end
Base.convert(::Type{Date},t::DateTime64{P}) where P = Date(1970)+P(t.i)
Base.convert(::Type{DateTime},t::DateTime64{P}) where P = DateTime(1970)+P(t.i)
Base.show(io::IO,t::DateTime64{P}) where P = print(io,"DateTime64[",P,"]: ",string(DateTime(t)))
Base.isless(x::DateTime64{P}, y::DateTime64{P}) where P = isless(x.i, y.i)
==(x::DateTime64{P}, y::DateTime64{P}) where P = x.i == y.i
strpairs = [Dates.Year => "Y", Dates.Month => "M", Dates.Week => "W", Dates.Day=>"D", 
    Dates.Hour => "h", Dates.Minute => "m", Dates.Second=>"s", Dates.Millisecond =>"ms",
    Dates.Microsecond => "us", Dates.Nanosecond => "ns"]
const jlperiod = Dict{String,Any}()
const pdt64string = Dict{Any, String}()
for p in strpairs
    jlperiod[p[2]] = p[1]
    pdt64string[p[1]] = p[2]
end
Base.convert(::Type{DateTime64{P}}, t::Date) where P = DateTime64{P}(Dates.value(P(t-Date(1970))))
Base.convert(::Type{DateTime64{P}}, t::DateTime) where P = DateTime64{P}(Dates.value(P(t-DateTime(1970))))
Base.convert(::Type{DateTime64{P}}, t::DateTime64{Q}) where {P,Q} = DateTime64{P}(Dates.value(P(Q(t.i))))
Base.zero(t::Union{DateTime64, Type{<:DateTime64}}) = t(0)
# Base.promote_rule(::Type{<:DateTime64{<:Dates.DatePeriod}}, ::Type{Date}) = Date 
# Base.promote_rule(::Type{<:DateTime64{<:Dates.DatePeriod}}, ::Type{DateTime}) = DateTime
# Base.promote_rule(::Type{<:DateTime64{<:Dates.TimePeriod}}, ::Type{Date}) = DateTime 
# Base.promote_rule(::Type{<:DateTime64{<:Dates.TimePeriod}}, ::Type{DateTime}) = DateTime



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
typestr(::Type{<:DateTime64{P}}) where P = "<M8[$(pdt64string[P])]"

const typestr_regex = r"^([<|>])([tbiufcmMOSUV])(\d*)(\[\w+\])?$"
const typemap = Dict{Tuple{Char, Int}, DataType}(
    ('b', 1) => Bool,
    ('S', 1) => ASCIIChar,
    ('U', 1) => Char,
)
sizemapf(x::Type{<:Number}) = sizeof(x)
sizemapf(x::Type{<:Complex{T}}) where T = sizeof(T)
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
            return Vector{sourcetype(first(filterlist))}
        end
        isempty(typesize) && throw((ArgumentError("$s is not a valid numpy typestr")))
        tc, ts = first(typecode), parse(Int, typesize)
        if (tc in ('U','S')) && ts > 1
          return MaxLengthString{ts,tc=='U' ? UInt32 : UInt8}
        end
        if tc == 'M' && ts == 8
            #We have a datetime64 value
            return DateTime64{jlperiod[String(typespec)[2:end-1]]}
        end
        # convert typecode to Char and typesize to Int
        typemap[(tc,ts)]
    end
end

"""Metadata configuration of the stored array

Each array requires essential configuration metadata to be stored, enabling correct
interpretation of the stored data. This metadata is encoded using JSON and stored as the
value of the “.zarray” key within an array store.

https://zarr.readthedocs.io/en/stable/spec/v2.html#metadata
"""
struct Metadata{T, N, C, F}
    zarr_format::Int
    shape::Base.RefValue{NTuple{N, Int}}
    chunks::NTuple{N, Int}
    dtype::String  # structured data types not yet supported
    compressor::C
    fill_value::Union{T, Nothing}
    order::Char
    filters::F  # not yet supported
    function Metadata{T2, N, C, F}(zarr_format, shape, chunks, dtype, compressor,fill_value, order, filters) where {T2,N,C,F}
        #We currently only support version 
        zarr_format == 2 || throw(ArgumentError("Zarr.jl currently only support v2 of the protocol"))
        #Do some sanity checks to make sure we have a sane array
        any(<(0), shape) && throw(ArgumentError("Size must be positive"))
        any(<(1), chunks) && throw(ArgumentError("Chunk size must be >= 1 along each dimension"))
        order === 'C' || throw(ArgumentError("Currently only 'C' storage order is supported"))
        new{T2, N, C, F}(zarr_format, Base.RefValue{NTuple{N,Int}}(shape), chunks, dtype, compressor,fill_value, order, filters)
    end
end

#To make unit tests pass with ref shape
import Base.==
function ==(m1::Metadata, m2::Metadata)
  m1.zarr_format == m2.zarr_format &&
  m1.shape[] == m2.shape[] &&
  m1.chunks == m2.chunks &&
  m1.dtype == m2.dtype &&
  m1.compressor == m2.compressor &&
  m1.fill_value == m2.fill_value &&
  m1.order == m2.order &&
  m1.filters == m2.filters
end


"Construct Metadata based on your data"
function Metadata(A::AbstractArray{T, N}, chunks::NTuple{N, Int};
        zarr_format::Integer=2,
        compressor::C=BloscCompressor(),
        fill_value::Union{T, Nothing}=nothing,
        order::Char='C',
        filters::Nothing=nothing,
        fill_as_missing = false,
    ) where {T, N, C}
    T2 = (fill_value === nothing || !fill_as_missing) ? T : Union{T,Missing}
    Metadata{T2, N, C, typeof(filters)}(
        zarr_format,
        size(A),
        chunks,
        typestr(eltype(A)),
        compressor,
        fill_value,
        order,
        filters
    )
end

Metadata(s::Union{AbstractString, IO},fill_as_missing) = Metadata(JSON.parse(s),fill_as_missing)

"Construct Metadata from Dict"
function Metadata(d::AbstractDict, fill_as_missing)
    # create a Metadata struct from it

    compdict = d["compressor"]
    compressor = getCompressor(compdict)

    filters = getfilters(d)

    T = typestr(d["dtype"], filters)
    N = length(d["shape"])
    C = typeof(compressor)
    F = typeof(filters)

    fv = fill_value_decoding(d["fill_value"], T)

    TU = (fv === nothing || !fill_as_missing) ? T : Union{T,Missing}

    Metadata{TU, N, C, F}(
        d["zarr_format"],
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
function JSON.lower(md::Metadata)
    Dict{String, Any}(
        "zarr_format" => md.zarr_format,
        "shape" => md.shape[] |> reverse,
        "chunks" => md.chunks |> reverse,
        "dtype" => md.dtype,
        "compressor" => md.compressor,
        "fill_value" => fill_value_encoding(md.fill_value),
        "order" => md.order,
        "filters" => md.filters
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

Base.eltype(::Metadata{T}) where T = T

# this correctly parses "NaN" and "Infinity"
fill_value_decoding(v::AbstractString, T::Type{<:Number}) = parse(T, v)
fill_value_decoding(v::Nothing, ::Any) = v
fill_value_decoding(v, T) = T(v)
fill_value_decoding(v, ::Type{ASCIIChar}) = v == "" ? nothing : v
