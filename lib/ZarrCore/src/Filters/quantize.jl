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

function QuantizeFilter(; digits = 10, T = Float16, Tenc = T)
    return QuantizeFilter{T, Tenc}(digits)
end

QuantizeFilter{T, Tenc}(; digits = 10) where {T, Tenc} = QuantizeFilter{T, Tenc}(digits)
QuantizeFilter{T}(; digits = 10) where T = QuantizeFilter{T, T}(digits)

function zencode(data::AbstractArray, filter::QuantizeFilter{DecodingType, EncodingType}) where {DecodingType, EncodingType}
    arr = reinterpret(DecodingType, vec(data))

    precision = 10.0^(-filter.digits)

    _exponent = log(10, precision) # log 10 in base `precision`
    exponent = _exponent < 0 ? floor(Int, _exponent) : ceil(Int, _exponent)

    bits = ceil(log(2, 10.0^(-exponent)))
    scale = 2.0^bits

    enc = @. convert(EncodingType, round(scale * arr) / scale)

    return enc
end

# Decoding is a no-op; quantization is a lossy filter but data is encoded directly.
function zdecode(data::AbstractArray, filter::QuantizeFilter{DecodingType, EncodingType}) where {DecodingType, EncodingType}
    return data
end

function JSON.lower(filter::QuantizeFilter{T, Tenc}) where {T, Tenc}
    return Dict("id" => "quantize", "digits" => filter.digits, "dtype" => typestr(T), "astype" => typestr(Tenc))
end

function getfilter(::Type{<: QuantizeFilter}, d)
    return QuantizeFilter{typestr(d["dtype"], typestr(d["astype"]))}(; digits = d["digits"])
end

filterdict["quantize"] = QuantizeFilter