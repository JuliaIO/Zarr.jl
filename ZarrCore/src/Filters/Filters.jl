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

Register each filter type under its Zarr `"id"` with
[`register_filter`](@ref).


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

"""Filter types keyed by their Zarr specification `"id"`."""
const filterdict = Dict{String,Type{<:Filter}}()

"""
    register_filter(name::String, ::Type{T}) where {T<:Filter}

Register filter type `T` under the Zarr specification name `name`.
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
