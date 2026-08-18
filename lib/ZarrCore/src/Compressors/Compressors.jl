import JSON # for JSON.lower

_reinterpret(::Type{T}, x::AbstractArray{S, 0}) where {T, S} = reinterpret(T, reshape(x, 1))
_reinterpret(::Type{T}, x::AbstractArray) where T = reinterpret(T, x)


const compressortypes = Dict{Union{String,Nothing}, Type{<: Compressor}}()

# The compressor interface. These generics are declared here, without any
# method, because the concrete compressors live in their own packages
# (`ZarrBlosc`, `ZarrZlib`, `ZarrZstd`, ...) and extend them from the outside:
# a subpackage writes `ZarrCore.zcompress(a, ::MyCompressor)`, which needs the
# generic to already exist here to attach a method to.
function getCompressor end
function zcompress end
function zuncompress end
function zcompress! end
function zuncompress! end
# JSON.lower is neither defined nor documented here, since that would be documentation piracy :yarr:

"""
    v2_to_v3_codecs(compressor, typesize::Int)

Translate a zarr v2 `compressor` into the tuple of zarr v3 bytes->bytes codecs
that reproduces it, given `typesize`, the size in bytes of one element of the
array the codec pipeline will be attached to.

This is the extension point a compressor implements in order to be usable in
zarr v3: define a method for your compressor type that returns a tuple of v3
codecs. [`NoCompressor`](@ref) maps to the empty tuple, and a compressor
without a method throws an `ArgumentError`.

The method for a compressor belongs in the same package as the compressor
itself -- see `ZarrBlosc`, `ZarrZlib` and `ZarrZstd`, each of which defines its
v2 compressor, its v3 codec, and the `v2_to_v3_codecs` method joining them.
"""
function v2_to_v3_codecs(compressor, typesize::Int)
    throw(ArgumentError("Unsupported compressor type for v3: $(typeof(compressor))"))
end

# ## Fallback definitions for the compressor interface
# Define fallbacks and generic methods for the compressor interface
getCompressor(compdict::Dict) = haskey(compdict, "id") ?
    getCompressor(compressortypes[compdict["id"]], compdict) :
    getCompressor(compressortypes[compdict["name"]], compdict["configuration"])
getCompressor(::Nothing) = NoCompressor()

# Compression when no filter is given
zcompress!(compressed,data,c,::Nothing) = zcompress!(compressed,data,c)
zuncompress!(data,compressed,c,::Nothing) = zuncompress!(data,compressed,c)

# Bulk `resize!` + `copyto!` (not `append!`): avoids elementwise growth over `NoCompressor`'s view.
function zcompress!(compressed, data, c)
    src = zcompress(data, c)
    resize!(compressed, length(src))
    copyto!(compressed, src)
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

# Fast path: bulk `unsafe_copyto!` avoids the elementwise `ReinterpretArray` copy of the fallback.
function zuncompress!(data::Array{T}, compressed::Vector{UInt8}, ::NoCompressor) where {T}
    isbitstype(T) || return copyto!(data, _reinterpret(T, compressed))
    n = sizeof(data)
    n == length(compressed) || throw(DimensionMismatch(
        "Encoded byte length $(length(compressed)) does not match output byte size $n"
    ))
    GC.@preserve data compressed unsafe_copyto!(Ptr{UInt8}(pointer(data)),
                                                pointer(compressed), n)
    return data
end

JSON.lower(::NoCompressor) = nothing

compressortypes[nothing] = NoCompressor

v2_to_v3_codecs(::NoCompressor, typesize::Int) = ()

"""
    DEFAULT_COMPRESSOR::Ref{Compressor}

Holds the compressor that is used when the caller does not specify one: every
`compressor` keyword argument in this package defaults to
`DEFAULT_COMPRESSOR[]`.

It is set by whichever package defines the default. A bare `ZarrCore` knows
only `NoCompressor`, so that is what it starts out with; the `Zarr` umbrella
package assigns `ZarrBlosc.BloscCompressor()` in its `__init__`, which is what
makes Blosc the default for anyone who does `using Zarr`.

Because the reference is only typed as the abstract `Compressor`, reading it
makes array creation type-unstable. That cost is paid once per array, when its
metadata is built -- not once per chunk: the metadata struct is parametrised on
the concrete compressor type, so every subsequent chunk (de)compression
dispatches statically.
"""
const DEFAULT_COMPRESSOR = Ref{Compressor}(NoCompressor())
