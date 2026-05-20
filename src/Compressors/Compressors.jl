import JSON # for JSON.lower

_reinterpret(::Type{T}, x::AbstractArray{S, 0}) where {T, S} = reinterpret(T, reshape(x, 1))
_reinterpret(::Type{T}, x::AbstractArray) where T = reinterpret(T, x)


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
getCompressor(compdict::Dict) = haskey(compdict, "id") ?
    getCompressor(compressortypes[compdict["id"]], compdict) :
    getCompressor(compressortypes[compdict["name"]], compdict["configuration"])
getCompressor(::Nothing) = NoCompressor()

# Compression when no filter is given
zcompress!(compressed,data,c,::Nothing) = zcompress!(compressed,data,c)
zuncompress!(data,compressed,c,::Nothing) = zuncompress!(data,compressed,c)

# Fallback definition of mutating form of compress and uncompress.
# `zcompress(data, c)` may return either a freshly-allocated `Vector{UInt8}`
# (real compressors) or a lazy `reinterpret` view (`NoCompressor`). For the
# view case, the old `empty!` + `append!` path walked the view element by
# element through `_growend!` / `push!`, materialising bytes one at a time;
# `resize!` + `copyto!` issues a single bulk copy (SIMD / memcpy under the
# hood) instead.
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

# Fast path: NoCompressor decode is a bulk byte copy. The generic
# `zuncompress!` fallback at the top of this file ends up at
# `copyto!(::Array{T}, ::ReinterpretArray)`, which walks element by
# element at ~17 GB/s on Apple silicon vs. ~85 GB/s for `unsafe_copyto!`.
# Mirror image of the encode-side bulk copy inside `zcompress!`.
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
