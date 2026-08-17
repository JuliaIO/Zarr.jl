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
This must also follow the Zarr specification's name for that compressor.  The name of the
compressor is the key, and the value is the compressor type (e.g. `BloscCompressor` or
`NoCompressor`).

For example, the Blosc compressor is named "blosc" in the Zarr spec, so the entry for
[`BloscCompressor`](@ref) must be added to `compressortypes` as
`compressortypes["blosc"] = BloscCompressor`.
"""
abstract type Compressor end

abstract type AbstractCodecPipeline end

"""
V2Pipeline wraps the existing v2 compressor + filter pair.
Delegates to zcompress!/zuncompress! with zero behavior change.
"""
struct V2Pipeline{C<:Compressor, F} <: AbstractCodecPipeline
    compressor::C
    filters::F
end

"""
V3Pipeline holds a three-phase v3 codec chain:
- array_array: tuple of array->array codecs (e.g. transpose)
- array_bytes: single array->bytes codec (e.g. bytes, sharding_indexed)
- bytes_bytes: tuple of bytes->bytes codecs (e.g. gzip, blosc, crc32c)
"""
struct V3Pipeline{AA, AB, BB} <: AbstractCodecPipeline
    array_array::AA
    array_bytes::AB
    bytes_bytes::BB
end

# Declare pipeline_encode and pipeline_decode! as generic functions.
# Methods are added in pipeline.jl after Codecs is loaded.
function pipeline_encode end
function pipeline_decode! end
