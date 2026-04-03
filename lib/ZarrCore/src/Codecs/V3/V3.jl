module V3Codecs

import ..Codecs: zencode, zdecode, zencode!, zdecode!
using JSON: JSON

abstract type V3Codec{In,Out} end

"""
Registry mapping V3 codec names (strings) to parser functions.
Each parser function takes a config dict and returns a V3Codec instance.
"""
const v3_codec_parsers = Dict{String, Function}()

"""
    codec_to_dict(c::V3Codec) -> Dict{String,Any}

Serialize a V3Codec to a JSON-compatible dictionary.
All V3Codec subtypes should implement this method.
"""
function codec_to_dict end

"""
    codec_category(::V3Codec{In,Out}) -> Tuple{Symbol,Symbol}

Return the (input, output) category of a codec, e.g. (:array, :bytes).
"""
codec_category(::V3Codec{In,Out}) where {In,Out} = (In, Out)

"""
    encoded_shape(codec::V3Codec, sz::NTuple{N,Int}) -> NTuple{N,Int}

Return the shape of the output of `codec_encode(codec, data)` given the input shape.
Default implementation returns the input shape unchanged.
"""
encoded_shape(::V3Codec, sz::NTuple{N,Int}) where {N} = sz

# --- BytesCodec (array -> bytes) ---

struct BytesCodec <: V3Codec{:array, :bytes}
    endian::Symbol  # :little or :big
    function BytesCodec(endian::Symbol)
        endian in (:little, :big) ||
            throw(ArgumentError("BytesCodec endian must be :little or :big, got :$endian"))
        new(endian)
    end
end
BytesCodec() = BytesCodec(:little)

const _SYSTEM_LITTLE_ENDIAN = Base.ENDIAN_BOM == 0x04030201
_needs_bswap(endian::Symbol) = (endian == :little) != _SYSTEM_LITTLE_ENDIAN

function codec_encode(c::BytesCodec, data::AbstractArray)
    if _needs_bswap(c.endian)
        return reinterpret(UInt8, bswap.(vec(data))) |> collect
    else
        return reinterpret(UInt8, vec(data)) |> collect
    end
end

function codec_decode(c::BytesCodec, encoded::Vector{UInt8}, ::Type{T}, shape::NTuple{N,Int}) where {T, N}
    arr = collect(reinterpret(T, encoded))
    if _needs_bswap(c.endian)
        arr = bswap.(arr)
    end
    return reshape(arr, shape)
end

function codec_to_dict(c::BytesCodec)
    Dict{String,Any}(
        "name" => "bytes",
        "configuration" => Dict{String,Any}("endian" => string(c.endian))
    )
end

# Register BytesCodec parser
v3_codec_parsers["bytes"] = function(config)
    endian = Symbol(get(config, "endian", "little"))
    BytesCodec(endian)
end

# --- TransposeCodec (array -> array) ---

struct TransposeCodec{N} <: V3Codec{:array, :array}
    order::NTuple{N, Int}  # permutation (1-based Julia indexing)
end

encoded_shape(c::TransposeCodec, sz::NTuple{N,Int}) where {N} = ntuple(i -> sz[c.order[i]], Val{N}())

function codec_encode(c::TransposeCodec, data::AbstractArray)
    return permutedims(data, c.order)
end

function codec_decode(c::TransposeCodec, encoded::AbstractArray)
    inv_order = Tuple(invperm(collect(c.order)))
    return permutedims(encoded, inv_order)
end

function codec_to_dict(c::TransposeCodec)
    # Zarr v3 spec uses 0-based C-order for the transpose order
    Dict{String,Any}(
        "name" => "transpose",
        "configuration" => Dict{String,Any}("order" => collect(c.order .- 1))
    )
end

# Note: TransposeCodec is NOT registered in v3_codec_parsers here because
# parsing requires shape context from metadata3.jl

end
