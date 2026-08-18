module Codecs

using JSON: JSON

"""
    abstract type Codec

The abstract supertype for all Zarr codecs

## Interface

All subtypes of `Codec` SHALL implement the following methods:

- `zencode(a, c::Codec)`: compress the array `a` using the codec `c`.
- `zdecode(a, c::Codec, T)`: decode the array `a` using the codec `c` 
  and return an array of type `T`.
- `JSON.lower(c::Codec)`: return a JSON representation of the codec `c`, which 
  follows the Zarr specification for that codec.
- `getCodec(::Type{<:Codec}, d::Dict)`: return a codec object from a given 
  dictionary `d` which contains the codec's parameters according to the Zarr spec.

Subtypes of `Codec` MAY also implement the following methods:

- `zencode!(encoded, data, c::Codec)`: encode the array `data` using the 
  codec `c` and store the result in the array `encoded`.
- `zdecode!(data, encoded, c::Codec)`: decode the array `encoded` 
  using the codec `c` and store the result in the array `data`.

Finally, each codec type MUST be registered in the registry for its Zarr format version, keyed by
the name the Zarr specification gives that codec.  For v3 the registry is
[`V3Codecs.codec_parsers`](@ref) and entries are added with
[`V3Codecs.register_codec`](@ref), which takes the spec name, the codec type, and a parser function
building the codec from its `"configuration"` dictionary.

For example, the Blosc codec is named "blosc" in the Zarr spec, so `ZarrBlosc.BloscV3Codec`
is registered as `register_codec("blosc", BloscV3Codec) do config, ctx ... end`.

!!! warning
    A codec defined *outside* `ZarrCore` must call `register_codec` from its module's
    `__init__`, not at top level. `codec_parsers` belongs to `ZarrCore`, and a mutation of
    another package's global state made while a module body runs is discarded when the
    precompiled image is written out -- silently, so the codec would simply be missing
    from the registry in every fresh session.
"""

abstract type Codec end

zencode(a, c::Codec) = error("Unimplemented")
zencode!(encoded, data, c::Codec) = error("Unimplemented")
zdecode(a, c::Codec, T::Type) = error("Unimplemented")
zdecode!(data, encoded, c::Codec) = error("Unimplemented")
JSON.lower(c::Codec) = error("Unimplemented")
getCodec(::Type{<:Codec}, d::Dict) = error("Unimplemented")

include("V3/V3.jl")

@static if VERSION >= v"1.11"
    include("public_names_codecs.jl")
end

end
