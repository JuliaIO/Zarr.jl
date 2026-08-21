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

Register each codec under its Zarr specification name. External packages must
register from `__init__` so the entry is restored after precompilation.
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
