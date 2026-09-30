# ## Filters as Zarr v3 codecs
#
# zarr-python exposes numcodecs filters to v3 as `numcodecs.<id>` codecs whose
# configuration is the v2 filter dict without `"id"`.

"""
    FilterCodec{In,Out}(filter::Filter)

Wraps a v2 [`Filter`](@ref) as a Zarr v3 `numcodecs.<id>` codec. Array filters
are `array -> array` codecs and byte filters (shuffle, fletcher32) are
`bytes -> bytes` codecs.
"""
struct FilterCodec{In,Out,F<:Filter} <: Codecs.V3Codecs.V3Codec{In,Out}
    filter::F
end
FilterCodec{In,Out}(f::F) where {In,Out,F<:Filter} = FilterCodec{In,Out,F}(f)

const _array_filters = ("delta", "fixedscaleoffset", "quantize")
const _bytes_filters = ("shuffle", "fletcher32")

_filter_id(c::FilterCodec) = JSON.lower(c.filter)["id"]
Codecs.V3Codecs.name(c::FilterCodec) = "numcodecs." * _filter_id(c)
Codecs.V3Codecs.encoded_type(c::FilterCodec{:array,:array}, ::Type) = desttype(c.filter)

function JSON.lower(c::FilterCodec)
    config = JSON.lower(c.filter)
    delete!(config, "id")
    Dict("name" => Codecs.V3Codecs.name(c), "configuration" => config)
end

Codecs.V3Codecs.codec_encode(c::FilterCodec{:array,:array}, data::AbstractArray) =
    reshape(zencode(data, c.filter), size(data))
Codecs.V3Codecs.codec_decode(c::FilterCodec{:array,:array}, encoded::AbstractArray) =
    reshape(zdecode(encoded, c.filter), size(encoded))
Codecs.V3Codecs.codec_encode(c::FilterCodec{:bytes,:bytes}, data::Vector{UInt8}) =
    collect(UInt8, zencode(data, c.filter))
Codecs.V3Codecs.codec_decode(c::FilterCodec{:bytes,:bytes}, encoded::Vector{UInt8}) =
    collect(UInt8, zdecode(encoded, c.filter))

for (ids, In, Out) in ((_array_filters, :array, :array), (_bytes_filters, :bytes, :bytes))
    for id in ids
        Codecs.V3Codecs.register_codec(id, FilterCodec{In,Out}) do config, ctx
            FilterCodec{In,Out}(getfilter(filterdict[id], config))
        end
    end
end

"""
    v3_filter_codecs(filters)

Convert v2 `filters` to `(array_array, bytes_bytes)` v3 codec tuples.
`VLenUTF8Filter` is dropped since v3 strings use the `vlen-utf8` codec.
"""
v3_filter_codecs(::Nothing) = ((), ())
function v3_filter_codecs(filters)
    codecs = map(filter(f -> !(f isa VLenUTF8Filter), collect(filters))) do f
        id = JSON.lower(f)["id"]
        id in _array_filters ? FilterCodec{:array,:array}(f) :
        id in _bytes_filters ? FilterCodec{:bytes,:bytes}(f) :
        throw(ArgumentError("Filter $id is not supported for Zarr v3"))
    end
    return (Tuple(filter(c -> c isa Codecs.V3Codecs.V3Codec{:array,:array}, codecs)),
            Tuple(filter(c -> c isa Codecs.V3Codecs.V3Codec{:bytes,:bytes}, codecs)))
end
