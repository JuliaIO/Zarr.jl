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

Finally, an entry MUST be added to the `filterdict` dictionary for each filter type.  
This must also follow the Zarr specification's name for that filter.  The name of the filter
is the key, and the value is the filter type (e.g. `VLenUInt8Filter` or `Fletcher32Filter`).


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

filterdict = Dict{String,Type{<:Filter}}()

function getfilters(d::Dict) 
    if !haskey(d,"filters")
        return nothing
    else
        if d["filters"] === nothing || isempty(d["filters"])
            return nothing
        end
        f = map(d["filters"]) do f
            if haskey(filterdict, f["id"])
                getfilter(filterdict[f["id"]], f)
            elseif haskey(compressortypes, f["id"])
                getCompressor(compressortypes[f["id"]], f)
            else
                throw(ArgumentError("filter `$(get(f, "id", ""))` ($f) not found"))
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
