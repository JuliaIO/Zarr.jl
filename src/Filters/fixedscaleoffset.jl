
"""
    FixedScaleOffsetFilter{T,TENC}(scale, offset)

A compressor that scales and offsets the data.
"""
struct FixedScaleOffsetFilter{ScaleOffsetType, T, Tenc} <: Filter{T, Tenc}
    scale::ScaleOffsetType
    offset::ScaleOffsetType
end

FixedScaleOffsetFilter{T}(scale::ScaleOffsetType, offset::ScaleOffsetType) where {T, ScaleOffsetType} = FixedScaleOffsetFilter{T, ScaleOffsetType}(scale, offset)
FixedScaleOffsetFilter(scale::ScaleOffsetType, offset::ScaleOffsetType) where {ScaleOffsetType} = FixedScaleOffsetFilter{ScaleOffsetType, ScaleOffsetType}(scale, offset)

function FixedScaleOffsetFilter(; scale::ScaleOffsetType, offset::ScaleOffsetType, T, Tenc = T) where ScaleOffsetType
    return FixedScaleOffsetFilter{ScaleOffsetType, T, Tenc}(scale, offset)
end

function zencode(a::AbstractArray, c::FixedScaleOffsetFilter{ScaleOffsetType, T, Tenc}) where {T, Tenc, ScaleOffsetType}
    return @. convert(Tenc, # convert to the encoding type after applying the scale and offset
        round((a - c.offset) * c.scale) # apply scale and offset, and round to nearest integer
    )
end

function zdecode(a::AbstractArray, c::FixedScaleOffsetFilter{ScaleOffsetType, T, Tenc}) where {T, Tenc, ScaleOffsetType}
    return _reinterpret(Base.nonmissingtype(T), @. a / c.scale + c.offset)
end


function getFilter(::Type{<: FixedScaleOffsetFilter}, d::Dict)
    scale = d["scale"]
    offset = d["offset"]
    # Types must be converted from strings to the actual Julia types they represent.
    string_T = d["dtype"]
    string_Tenc = get(d, "atype", string_T)
    T = typestr(string_T)
    Tenc = typestr(string_Tenc)
    return FixedScaleOffsetFilter{T, Tenc}(scale, offset)
end

function JSON.lower(c::FixedScaleOffsetFilter{ScaleOffsetType, T, Tenc}) where {ScaleOffsetType, T, Tenc}
    return Dict("id" => "fixedscaleoffset", "scale" => c.scale, "offset" => c.offset, "dtype" => typestr(T), "atype" => typestr(Tenc))
end

filterdict["fixedscaleoffset"] = FixedScaleOffsetFilter
