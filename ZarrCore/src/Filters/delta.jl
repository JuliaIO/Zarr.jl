#=
# Delta compression


=#

"""
    DeltaFilter(; DecodingType, [EncodingType = DecodingType])

Delta-based compression for Zarr arrays. Encoding stores the first value followed
by successive differences. Decoding returns `DecodingType` values and preserves
integer wraparound when both types are the same.
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
    decoded = similar(encoded, DecodingType)
    isempty(encoded) && return decoded
    decoded[begin] = encoded[begin]
    @views accumulate!(+, decoded[begin+1:end], encoded[begin+1:end]; init=decoded[begin])
    return decoded
end

function JSON.lower(filter::DeltaFilter{T, Tenc}) where {T, Tenc}
    return Dict("id" => "delta", "dtype" => typestr(T), "astype" => typestr(Tenc))
end

function getfilter(::Type{<: DeltaFilter}, d)
    return DeltaFilter{typestr(d["dtype"], haskey(d, "astype") ? typestr(d["astype"]) : d["dtype"])}()
end

register_filter("delta", DeltaFilter)