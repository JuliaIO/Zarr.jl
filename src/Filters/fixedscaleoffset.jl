
"""
    FixedScaleOffsetFilter{T,TENC}(scale, offset)

A compressor that scales and offsets the data.

!!! note
    The geographic CF standards define scale/offset decoding as `x * scale + offset`,
    but this filter defines it as `x / scale + offset`.  Constructing a `FixedScaleOffsetFilter`
    from CF data means `FixedScaleOffsetFilter(1/cf_scale_factor, cf_add_offset)`.
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
    if Tenc <: Integer
        return [round(Tenc, (a - c.offset) * c.scale) for a in a] # apply scale and offset, and round to nearest integer
    else
        return [convert(Tenc, (a - c.offset) * c.scale) for a in a] # apply scale and offset
    end
end

function zdecode(a::AbstractArray, c::FixedScaleOffsetFilter{ScaleOffsetType, T, Tenc}) where {T, Tenc, ScaleOffsetType}
    return [convert(Base.nonmissingtype(T), (a / c.scale) + c.offset) for a in a]
end


function getfilter(::Type{<: FixedScaleOffsetFilter}, d::Dict)
    scale = d["scale"]
    offset = d["offset"]
    # Types must be converted from strings to the actual Julia types they represent.
    string_T = d["dtype"]
    string_Tenc = get(d, "astype", string_T)
    T = typestr(string_T)
    Tenc = typestr(string_Tenc)
    return FixedScaleOffsetFilter{Tenc, T, Tenc}(scale, offset)
end

function JSON.lower(c::FixedScaleOffsetFilter{ScaleOffsetType, T, Tenc}) where {ScaleOffsetType, T, Tenc}
    return Dict("id" => "fixedscaleoffset", "scale" => c.scale, "offset" => c.offset, "dtype" => typestr(T), "astype" => typestr(Tenc))
end

filterdict["fixedscaleoffset"] = FixedScaleOffsetFilter
