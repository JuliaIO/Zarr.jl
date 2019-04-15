
"""NumPy array protocol type string (typestr) format

A string providing the basic type of the homogenous array. The basic string format
consists of 3 parts: a character describing the byteorder of the data
(<: little-endian, >: big-endian, |: not-relevant), a character code giving the basic
type of the array, and an integer providing the number of bytes the type uses.

https://zarr.readthedocs.io/en/stable/spec/v2.html#data-type-encoding
"""

primitive type ASCIIChar <: AbstractChar 8 end
ASCIIChar(x::UInt8) = reinterpret(ASCIIChar, x)
UInt8(x::ASCIIChar) = reinterpret(UInt8, x)
Base.codepoint(x::ASCIIChar) = UInt8(x)
Base.show(io::IO, x::ASCIIChar) = print(io, Char(x))
Base.zero(ASCIIChar) = ASCIIChar(Base.zero(UInt8))


typestr(t::Type) = string('<', 'V', sizeof(t))
typestr(t::Type{>:Missing}) = typestr(Base.nonmissingtype(t))
typestr(t::Type{Bool}) = string('<', 'b', sizeof(t))
typestr(t::Type{<:Signed}) = string('<', 'i', sizeof(t))
typestr(t::Type{<:Unsigned}) = string('<', 'u', sizeof(t))
typestr(t::Type{Complex{T}} where T<:AbstractFloat) = string('<', 'c', sizeof(t))
typestr(t::Type{<:AbstractFloat}) = string('<', 'f', sizeof(t))

const typestr_regex = r"^([<|>])([tbiufcmMOSUV])(\d+)$"
const typemap = Dict{Tuple{Char, Int}, DataType}(
    ('b', 1) => Bool,
    ('S', 1) => ASCIIChar,
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


function typestr(s::AbstractString)
    m = match(typestr_regex, s)
    if m === nothing
        throw(ArgumentError("$s is not a valid numpy typestr"))
    else
        byteorder, typecode, typesize = m.captures
        if byteorder == ">"
            throw(ArgumentError("Big-endian data not yet supported"))
        end
        # convert typecode to Char and typesize to Int
        typemap[(first(typecode), parse(Int, typesize))]
    end
end

"""Metadata configuration of the stored array

Each array requires essential configuration metadata to be stored, enabling correct
interpretation of the stored data. This metadata is encoded using JSON and stored as the
value of the “.zarray” key within an array store.

https://zarr.readthedocs.io/en/stable/spec/v2.html#metadata
"""
struct Metadata{T, N, C}
    zarr_format::Int
    shape::NTuple{N, Int}
    chunks::NTuple{N, Int}
    dtype::String  # structured data types not yet supported
    compressor::C
    fill_value::Union{T, Nothing}
    order::Char
    filters::Nothing  # not yet supported
end

"Construct Metadata based on your data"
function Metadata(A::AbstractArray{T, N}, chunks::NTuple{N, Int};
        zarr_format::Integer=2,
        compressor::C=BloscCompressor(),
        fill_value::Union{T, Nothing}=nothing,
        order::Char='C',
        filters::Nothing=nothing
    ) where {T, N, C}
    T2 = fill_value === nothing ? T : Union{T,Missing}
    Metadata{T2, N, C}(
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

"Construct Metadata from JSON"
function Metadata(s::Union{AbstractString, IO})
    # get the JSON representation as a Dict
    d = JSON.parse(s)
    # create a Metadata struct from it

    compdict = d["compressor"]
    compressor = getCompressor(compdict)

    T = typestr(d["dtype"])
    N = length(d["shape"])
    C = typeof(compressor)

    fv = fill_value_decoding(d["fill_value"], T)

    TU = fv === nothing ? T : Union{T,Missing}

    Metadata{TU, N, C}(
        d["zarr_format"],
        NTuple{N, Int}(d["shape"]) |> reverse,
        NTuple{N, Int}(d["chunks"]) |> reverse,
        d["dtype"],
        compressor,
        fv,
        first(d["order"]),
        d["filters"]
    )
end

"Describes how to lower Metadata to JSON, used in json(::Metadata)"
function JSON.lower(md::Metadata)
    Dict{String, Any}(
        "zarr_format" => md.zarr_format,
        "shape" => md.shape |> reverse,
        "chunks" => md.chunks |> reverse,
        "dtype" => md.dtype,
        "compressor" => JSON.lower(md.compressor),
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
fill_value_decoding(v::Nothing, T) = v
fill_value_decoding(v, T) = T(v)
fill_value_decoding(v, T::Type{ASCIIChar}) = v == "" ? nothing : v
