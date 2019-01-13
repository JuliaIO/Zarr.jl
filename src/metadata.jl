
"""NumPy array protocol type string (typestr) format

A string providing the basic type of the homogenous array. The basic string format
consists of 3 parts: a character describing the byteorder of the data
(<: little-endian, >: big-endian, |: not-relevant), a character code giving the basic
type of the array, and an integer providing the number of bytes the type uses.

https://zarr.readthedocs.io/en/stable/spec/v2.html#data-type-encoding
"""
typestr(t::Type) = string('<', 'V', sizeof(t))
typestr(t::Type{Bool}) = string('<', 'b', sizeof(t))
typestr(t::Type{<:Signed}) = string('<', 'i', sizeof(t))
typestr(t::Type{<:Unsigned}) = string('<', 'u', sizeof(t))
typestr(t::Type{Complex{T}} where T<:AbstractFloat) = string('<', 'c', sizeof(t))
typestr(t::Type{<:AbstractFloat}) = string('<', 'f', sizeof(t))

const typestr_regex = r"^([<|>])([tbiufcmMOSUV])(\d+)$"
const typemap = Dict{Tuple{Char, Int}, DataType}(
    ('b', 1) => Bool,
    ('i', 1) => Int8,
    ('u', 1) => UInt8,
    ('c', 2) => Complex{Float16},
    ('c', 4) => Complex{Float32},
    ('c', 8) => Complex{Float64},
    ('f', 2) => Float16,
    ('f', 4) => Float32,
    ('f', 8) => Float64,
)

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
        order::Char='F',
        filters::Nothing=nothing
    ) where {T, N, C}
    Metadata{T, N, C}(
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
    compressor = getCompressor(compressortypes[compdict["id"]], compdict)

    T = typestr(d["dtype"])
    N = length(d["shape"])
    C = typeof(compressor)

    Metadata{T, N, C}(
        d["zarr_format"],
        NTuple{N, Int}(d["shape"]),
        NTuple{N, Int}(d["chunks"]),
        d["dtype"],
        compressor,
        fill_value_decoding(d["fill_value"], T),
        first(d["order"]),
        d["filters"]
    )
end

"Describes how to lower Metadata to JSON, used in json(::Metadata)"
function JSON.lower(md::Metadata)
    Dict{String, Any}(
        "zarr_format" => md.zarr_format,
        "shape" => md.shape,
        "chunks" => md.chunks,
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

function fill_value_encoding(v::AbstractFloat)
    if isnan(v)
        string(v)
    elseif isinf(v)
        string(v, "inity")
    else
        v
    end
end

# this correctly parses "NaN" and "Infinity"
fill_value_decoding(v::AbstractString, T::Type{<:Number}) = parse(T, v)
fill_value_decoding(v::Nothing, T) = v
fill_value_decoding(v, T) = T(v)
