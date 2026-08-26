import JSON # for JSON.lower

_reinterpret(::Type{T}, x::AbstractArray{S, 0}) where {T, S} = reinterpret(T, reshape(x, 1))
_reinterpret(::Type{T}, x::AbstractArray) where T = reinterpret(T, x)


const compressortypes = Dict{Union{String,Nothing}, Type{<: Compressor}}()

# Extension points implemented by compressor packages.
function getCompressor end
function zcompress end
function zuncompress end
function zcompress! end
function zuncompress! end

"""
    v2_to_v3_codecs(compressor, typesize::Int)

Return the v3 codec tuple equivalent to a v2 compressor. `typesize` is the
element size in bytes. Unsupported compressor types throw `ArgumentError`.
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

# `::Type{T}` (rather than a plain `T` argument) so that Julia specialises on the
# element type; `nonmissingtype` because a `fill_as_missing` array decodes into
# the `isbits` inner buffer of a `SenMissArray` (this matches what the ZarrZlib
# and ZarrZstd compressors do).
function zuncompress(a, ::NoCompressor, ::Type{T}) where {T}
  _reinterpret(Base.nonmissingtype(T),a)
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

# ## Statically typed compressor construction (juliac `--trim=safe` path)
#
# `getCompressor(::Type{C}, ::Union{Nothing,CompressorJSON})` is the typed
# counterpart of `getCompressor(compdict::Dict)`: the caller states which
# compressor type it expects, so the return type is concrete. Compressor
# packages add a `getCompressor(::Type{TheirCompressor}, ::CompressorJSON)`
# method; the two fallbacks below cover the "store and caller disagree" cases.
# The `Dict` methods are untouched and stay on the dynamic path.
#
# Error messages are built with `string(...)` from `String`s only: string
# interpolation of a non-`String` (a `DataType`, say) pulls in the whole
# `show` stack and is not `--trim=safe`.

"""
    getCompressor(::Type{C}, c::Union{Nothing,CompressorJSON}) where {C<:Compressor}

Build a compressor of the statically known type `C` from the `compressor` entry
of a parsed [`ZarrayJSON`](@ref) (`nothing` when the array is uncompressed).
Throws `ArgumentError` when the store's compressor does not match `C`.

This is the typed counterpart of `getCompressor(::Dict)` and is used by the
statically typed `zopen(::Type{T}, ::Val{N}, ...)` path.
"""
function getCompressor(::Type{C}, ::Nothing) where {C<:Compressor}
    throw(ArgumentError("the store has no compressor, but a compressor type was requested"))
end

getCompressor(::Type{NoCompressor}, ::Nothing) = NoCompressor()

function getCompressor(::Type{NoCompressor}, c::CompressorJSON)
    throw(ArgumentError(string("the store has a \"", c.id,
        "\" compressor, but NoCompressor was requested")))
end

"""
    codec_id(::Type{C}) where {C<:Compressor} -> String

The numcodecs `"id"` under which a compressor type is stored in a Zarr v2
`.zarray` document, e.g. `"zstd"`. [`NoCompressor`](@ref) returns `""`, because
an uncompressed array stores JSON `null` instead of a compressor object.

This is the static counterpart of the `compressortypes` registry: the
registry maps a stored id to a type at run time, `codec_id` maps a statically
known type to its id. Compressor packages define a method next to their
`getCompressor(::Type{C}, ::CompressorJSON)` method; defining one is the
extension point that admits a compressor into a `ZarrTrimmable` codec pool.
"""
function codec_id end

codec_id(::Type{NoCompressor}) = ""

compressortypes[nothing] = NoCompressor

v2_to_v3_codecs(::NoCompressor, typesize::Int) = ()

struct _DefaultCompressorFallback end

"""
    default_compressor()

Return the compressor used when none is specified. Bare `ZarrCore` returns
[`NoCompressor`](@ref); compressor packages may provide a zero-argument method.
"""
# The fallback remains less specific than a package's zero-argument method.
default_compressor(::_DefaultCompressorFallback...) = NoCompressor()
