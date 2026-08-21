import JSON

"""
    abstract type Filter{T,TENC}

The supertype for all Zarr filters.  

## Interface

All subtypes MUST implement the following methods:

- [`zencode(ain, filter::Filter)`](@ref zencode): Encodes data `ain` using the filter, and returns a vector of bytes.
- [`zdecode(ain, filter::Filter)`](@ref zdecode): Decodes data `ain`, a vector of bytes, using the filter, and returns the original data.
- [`JSON.lower`](@ref): Returns a JSON-serializable dictionary representing the filter, according to the Zarr specification.
- [`getfilter(::Type{<: Filter}, filterdict)`](@ref getfilter): Returns the filter type read from a given filter dictionary.

If the filter has type parameters, it MUST also implement:
- [`sourcetype(::Filter)::T`](@ref sourcetype): equivalent to `dtype` in the Python Zarr implementation.
- [`desttype(::Filter)::T`](@ref desttype): equivalent to `atype` in the Python Zarr implementation.

Finally, each filter type MUST be registered by calling
[`register_filter(name, FilterType)`](@ref register_filter).
The name must follow the Zarr specification's name for that filter, and the registered
value is the filter type (e.g. `VLenUInt8Filter` or `Fletcher32Filter`).


Subtypes include: [`VLenArrayFilter`](@ref), [`VLenUTF8Filter`](@ref), [`Fletcher32Filter`](@ref).
"""
abstract type Filter{T,TENC} end

"""
    zencode(ain, filter::Filter)

Encodes data `ain` using the filter, and returns a vector of bytes.
"""
function zencode end

"""
    zdecode(ain, filter::Filter)

Decodes data `ain`, a vector of bytes, using the filter, and returns the original data.
"""
function zdecode end

"""
    getfilter(::Type{<: Filter}, filterdict)

Returns the filter type read from a given specification dictionary, which must follow the Zarr specification.
"""
function getfilter end

"""
    sourcetype(::Filter)::T

Returns the source type of the filter.
"""
function sourcetype end

"""
    desttype(::Filter)::T

Returns the destination type of the filter.
"""
function desttype end

"""
Registry mapping filter names (as they appear in the `"id"` field of a filter
specification) to filter types. Use [`register_filter`](@ref) to add new entries.
"""
const filterdict = Dict{String,Type{<:Filter}}()

"""
    register_filter(name::String, ::Type{T}) where {T<:Filter}

Register the filter type `T` under `name`, which must be the name the Zarr
specification uses for that filter (i.e. the `"id"` field of a filter
specification).

Filters are instantiated from their specification dictionary via
[`getfilter`](@ref), so only the type itself is stored in the registry:

    register_filter("myfilter", MyFilter)
"""
function register_filter(name::String, ::Type{T}) where {T<:Filter}
    filterdict[name] = T
end

function getfilters(d::Dict) 
    if !haskey(d,"filters")
        return nothing
    else
        if d["filters"] === nothing || isempty(d["filters"])
            return nothing
        end
        f = map(d["filters"]) do f
            try
            getfilter(filterdict[f["id"]], f)
            catch e
                @show f
                rethrow(e)
            end
        end
        return (f...,)
    end
end
sourcetype(::Filter{T}) where T = T
desttype(::Filter{<:Any,T}) where T = T

zencode(ain,::Nothing) = ain

include("vlenfilters.jl")
include("fletcher32.jl")
include("fixedscaleoffset.jl")
include("shuffle.jl")
include("quantize.jl")
include("delta.jl")
