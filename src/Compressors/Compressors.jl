import JSON # for JSON.lower

_reinterpret(::Type{T}, x::AbstractArray{S, 0}) where {T, S} = reinterpret(T, reshape(x, 1))
_reinterpret(::Type{T}, x::AbstractArray) where T = reinterpret(T, x)

"""
    abstract type Compressor

The abstract supertype for all Zarr compressors.

## Interface

All subtypes of `Compressor` SHALL implement the following methods:

- `zcompress(a, c::Compressor)`: compress the array `a` using the compressor `c`.
- `zuncompress(a, c::Compressor, T)`: uncompress the array `a` using the compressor `c` 
  and return an array of type `T`.
- `JSON.lower(c::Compressor)`: return a JSON representation of the compressor `c`, which 
  follows the Zarr specification for that compressor.
- `getCompressor(::Type{<:Compressor}, d::Dict)`: return a compressor object from a given 
  dictionary `d` which contains the compressor's parameters according to the Zarr spec.

Subtypes of `Compressor` MAY also implement the following methods:

- `zcompress!(compressed, data, c::Compressor)`: compress the array `data` using the 
  compressor `c` and store the result in the array `compressed`.
- `zuncompress!(data, compressed, c::Compressor)`: uncompress the array `compressed` 
  using the compressor `c` and store the result in the array `data`.

Finally, an entry MUST be added to the `compressortypes` dictionary for each compressor type.  
This must also follow the Zarr specification's name for that compressor.  The name of the compressor
is the key, and the value is the compressor type (e.g. `BloscCompressor` or `NoCompressor`).

For example, the Blosc compressor is named "blosc" in the Zarr spec, so the entry for [`BloscCompressor`](@ref) 
must be added to `compressortypes` as `compressortypes["blosc"] = BloscCompressor`.
"""
abstract type Compressor end

const compressortypes = Dict{Union{String,Nothing}, Type{<: Compressor}}()

# function getCompressor end
# function zcompress end
# function zuncompress end
# function zcompress! end
# function zuncompress! end
# JSON.lower is neither defined nor documented here, since that would be documentation piracy :yarr:

# Include the compressor implementations
include("blosc.jl")
include("zlib.jl")
include("zstd.jl")

# ## Fallback definitions for the compressor interface
# Define fallbacks and generic methods for the compressor interface
getCompressor(compdict::Dict) = getCompressor(compressortypes[compdict["id"]],compdict)
getCompressor(::Nothing) = NoCompressor()

# Compression when no filter is given
zcompress!(compressed,data,c,::Nothing) = zcompress!(compressed,data,c)
zuncompress!(data,compressed,c,::Nothing) = zuncompress!(data,compressed,c)

# Fallback definition of mutating form of compress and uncompress
function zcompress!(compressed, data, c) 
    empty!(compressed)
    append!(compressed,zcompress(data, c))
end
zuncompress!(data, compressed, c) = copyto!(data, zuncompress(compressed, c, eltype(data)))


# Function given a filter stack
function zcompress!(compressed, data, c, f)
    a2 = foldl(f, init=data) do anow, fnow
        zencode(anow,fnow)
    end
    zcompress!(compressed, a2, c)
end

function zuncompress!(data, compressed, c, f)
    data2 = zuncompress(compressed, c, desttype(last(f))) 
    a2 = foldr(f, init = data2) do fnow, anow
        zdecode(anow, fnow)
    end
    copyto!(data, a2)
end

# ## `NoCompressor`
# The default and most minimal implementation of a compressor follows here, which does
# no actual compression.  This is a good reference implementation for other compressors.

"""
    NoCompressor()

Creates an object that can be passed to ZArray constructors without compression.
"""
struct NoCompressor <: Compressor end

function zuncompress(a, ::NoCompressor, T)
  _reinterpret(T,a)
end

function zcompress(a, ::NoCompressor)
  _reinterpret(UInt8,a)
end

JSON.lower(::NoCompressor) = nothing

compressortypes[nothing] = NoCompressor
