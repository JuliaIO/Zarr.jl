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

Register each compressor under its Zarr specification name in
[`compressortypes`](@ref). External packages must register from `__init__` so
the entry is restored after precompilation.

To be usable with Zarr v3 a compressor SHOULD additionally implement
[`v2_to_v3_codecs`](@ref), mapping it onto the equivalent v3 codecs.
"""
abstract type Compressor end

abstract type AbstractCodecPipeline end

"""Zarr v2 compressor and filter pipeline."""
struct V2Pipeline{C<:Compressor, F} <: AbstractCodecPipeline
    compressor::C
    filters::F
end

"""Zarr v3 array-to-array, array-to-bytes, and bytes-to-bytes codec pipeline."""
struct V3Pipeline{AA, AB, BB} <: AbstractCodecPipeline
    array_array::AA
    array_bytes::AB
    bytes_bytes::BB
end

# Implemented in pipeline.jl after Codecs loads.
function pipeline_encode end
function pipeline_decode! end
