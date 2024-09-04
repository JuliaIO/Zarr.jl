#=
# Delta compression


=#

"""
    DeltaFilter(; DecodingType, [EncodingType = DecodingType])

Delta-based compression for Zarr arrays.  (Delta encoding is Julia `diff`, decoding is Julia `cumsum`).
"""
struct DeltaFilter{T, TENC} <: Filter{T, TENC}
end

function DeltaFilter(; DecodingType = Float16, EncodingType = DecodingType)
    return DeltaFilter{DecodingType, EncodingType}()
end

DeltaFilter{T}() where T = DeltaFilter{T, T}()

function zencode(data::AbstractArray, filter::DeltaFilter{DecodingType, EncodingType}) where {DecodingType, EncodingType}
    arr = reinterpret(DecodingType, vec(data))

    enc = similar(arr, EncodingType)
    # perform the delta operation
    enc[begin] = arr[begin]
    enc[begin+1:end] .= diff(arr)
    return enc
end

function zdecode(data::AbstractArray, filter::DeltaFilter{DecodingType, EncodingType}) where {DecodingType, EncodingType}
    encoded = reinterpret(EncodingType, vec(data))
    decoded = DecodingType.(cumsum(encoded))
    return decoded
end

function JSON.lower(filter::DeltaFilter{T, Tenc}) where {T, Tenc}
    return Dict("type" => "delta", "dtype" => typestring(T), "atype" => typestring(Tenc))
end

function getfilter(::Type{<: DeltaFilter}, d)
    return DeltaFilter{typestr(d["dtype"], haskey(d, "atype") ? typestr(d["atype"]) : d["dtype"])}()
end

filterdict["delta"] = DeltaFilter