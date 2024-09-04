module Codecs

import JSON

"""
    abstract type Codec

The supertype for all Zarr codecs.  A codec specifies an invertible transform (encoding and decoding) between two representations of data.

## Interface

All codecs MUST implement the following methods:

- [`encode(codec::Codec, ain)`](@ref encode): Encodes data `ain` using the codec, and returns the encoded data.
- [`decode(codec::Codec, ain)`](@ref decode): Decodes data `ain`, the encoded data, using the codec, and returns the original data.
- [`JSON.lower`]: Returns a JSON-serializable dictionary (can be a `Dict`) representing the codec, according to the Zarr specification.
- [`getcodec(::Type{<: YourCodec}, filterdict)`](@ref getcodec): Returns the codec type read from a given codec dictionary.
- [`codecname(::Codec)`](@ref codecname): Returns the name of the codec as a String, according to the Zarr specification or in sync with the numcodecs name for it.

Additionally, they MUST add an entry to the `CODEC_REGISTRY` dictionary, with the key being the name of the codec and the value being the type of the codec, that can be passed into `getcodec`.

Depending on their definition, codecs MAY also implement:
- [`encodingtype(::Codec)`](@ref sourcetype): Returns the type of data when encoded.  Equivalent to `dtype` in Python.  Usually useful for [`ArrayToArray`](@ref) codecs.
- [`decodingtype(::Codec)`](@ref desttype): Returns the type of data when decoded.  Equivalent to `astype` in Python.  Usually useful for [`ArrayToArray`](@ref) or [`ArrayToBytes`](@ref) codecs.

## Extended help

## What's a codec?

From the Zarr v3 spec,

A codec specifies a bidirectional transform (an encode transform and a decode transform).

Each codec has an encoded representation and a decoded representation; each of these two representations are defined to be either:

- a multi-dimensional array of some shape and data type, or
- a byte string.

Based on the input and output representations for the encode transform, codecs can be classified as one of three kinds:
- [`ArrayToArray`](@ref): The input and output representations are both multi-dimensional arrays.
- [`ArrayToBytes`](@ref): The input representation is a multi-dimensional array and the output representation is a byte string.
- [`BytesToBytes`](@ref): The input and output representations are both byte strings.

Note that [`BytesToArray`](@ref) codecs are disallowed by the Zarr v3 spec at this point.  
However, the inverse transform of an ArrayToBytes codec must go from bytes to some array.

## Zarr v2 equivalent

In Zarr v2, codecs were divided into "filters" and "compressors".  The compressor was essentially the last filter, but with some extra steps.

However, they all had fundamentally similar interfaces.

"""
abstract type Codec end

"""
    abstract type ArrayToArrayCodec <: Codec end

The supertype for all array-to-array codecs.
"""
abstract type ArrayToArrayCodec <: Codec end

"""
    abstract type ArrayToBytesCodec <: Codec end

The supertype for all array-to-bytes codecs.
"""
abstract type ArrayToBytesCodec <: Codec end

"""
    abstract type BytesToBytesCodec <: Codec end

The supertype for all bytes-to-bytes codecs.
"""
abstract type BytesToBytesCodec <: Codec end

# Definitions of API functions

"""
    const CODEC_REGISTRY::Dict{String, Type{<:Codec}}

A dictionary of all the codec types that have been registered.  Keys are the codec names, and values are the codec types.
"""
const CODEC_REGISTRY = Dict{String, Type{<:Codec}}()

"""
    name(codec::Codec)

Returns the name of the codec as a String, according to the Zarr specification or in sync with the numcodecs name for it.
"""
function name(codec::Codec)
    error("Codec $codec has not implemented name(::Codec).  Please implement this method, or file an issue at https://github.com/JuliaIO/Zarr.jl/issues.")
end

"""
    getcodec(codectype::Type{<:Codec}, filterdict::Dict)

Returns the codec type read from a given codec dictionary.
"""
function getcodec(codectype::Type{<:Codec}, filterdict::Dict)
    error("Codec $codectype has not implemented getcodec(::Type{<:$codectype}, ::Dict).  Please implement this method, or file an issue at https://github.com/JuliaIO/Zarr.jl/issues.")
end

# Don't want to inject an extra docstring for this function since we don't own it and it could cause confusion for end users.
function JSON.lower(codec::Codec)
    error("Codec $codec has not implemented JSON.lower(::Codec).  Please implement this method, or file an issue at https://github.com/JuliaIO/Zarr.jl/issues.")
end

function encode(codec::Codec, ain::AbstractArray)
    error("Codec $codec has not implemented encode(::Codec, ::AbstractArray).  Please implement this method, or file an issue at https://github.com/JuliaIO/Zarr.jl/issues.")
end

function decode(codec::Codec, ain::AbstractArray)
    error("Codec $codec has not implemented decode(::Codec, ::AbstractArray).  Please implement this method, or file an issue at https://github.com/JuliaIO/Zarr.jl/issues.")
end

"""
    decode!(codec::Codec, aout::AbstractArray, ain::AbstractArray)

A more efficient version of `decode` that allows you to pass in an output array and decode into it.

Note that this defaults to calling `decode` and then copying the result to the output array, 
but codecs can implement this more efficiently, should they desire.
"""
function decode!(codec::Codec, aout::AbstractArray, ain::AbstractArray)
    aout .= decode(codec, ain)
end

end # module