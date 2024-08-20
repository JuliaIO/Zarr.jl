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



Subtypes include: [`VLenArrayFilter`](@ref), [`VLenUTF8Filter`](@ref), [`Fletcher32Filter`](@ref).
"""
abstract type Filter{T,TENC} end

function zencode end
function zdecode end
function getfilter end
function sourcetype end
function desttype end

filterdict = Dict{String,Type{<:Filter}}()

function getfilters(d::Dict) 
    if !haskey(d,"filters")
        return nothing
    else
        if d["filters"] === nothing || isempty(d["filters"])
            return nothing
        end
        f = map(d["filters"]) do f
            getfilter(filterdict[f["id"]], f)
        end
        return (f...,)
    end
end
sourcetype(::Filter{T}) where T = T
desttype(::Filter{<:Any,T}) where T = T

zencode(ain,::Nothing) = ain

include("vlenfilters.jl")

