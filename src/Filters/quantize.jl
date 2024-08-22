#=
# Quantize compression


=#

"""
    QuantizeFilter(; digits, DecodingType, [EncodingType = DecodingType])

Quantization based compression for Zarr arrays.
"""
struct QuantizeFilter{T, TENC} <: Filter{T, TENC}
    digits::Int32
end

function QuantizeFilter(; digits = 10, T = Float16, Tenc = DecodingType)
    return QuantizeFilter{T, Tenc}(digits)
end

function zencode(data::AbstractArray, filter::QuantizeFilter{DecodingType, EncodingType}) where {DecodingType, EncodingType}
    arr = reinterpret(DecodingType, vec(data))

    precision = 10^(-filter.digits)

    _exponent = log(precision, 10)
    exponent = _exponent < 0 ? floor(Int, _exponent) : ceil(Int, _exponent)

    bits = ceil(log(10^(-exponent), 2))
    scale = 2^bits

    enc = @. round(scale * arr) / scale

    if EncodingType == DecodingType
        return enc
    else
        return reinterpret(EncodingType, enc)
    end
end

function zdecode(data::AbstractArray, filter::QuantizeFilter{DecodingType, EncodingType}) where {DecodingType, EncodingType}
    return data
end