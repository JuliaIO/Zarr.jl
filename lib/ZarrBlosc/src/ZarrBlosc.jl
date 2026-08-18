"""
    ZarrBlosc

Blosc support for Zarr.jl: the zarr v2 [`BloscCompressor`](@ref) and the
matching zarr v3 [`BloscV3Codec`](@ref), both backed by Blosc.jl.

This is a subpackage of Zarr.jl; its public API is re-exported by `Zarr`, so
`Zarr.BloscCompressor` keeps working exactly as before. `Zarr` also makes
`BloscCompressor()` the default compressor (see `ZarrCore.DEFAULT_COMPRESSOR`);
a bare `ZarrCore` without this package defaults to `ZarrCore.NoCompressor()`.
"""
module ZarrBlosc

import Blosc
import JSON # for JSON.lower

# Only the names that are used unqualified live here. Methods that *extend* a
# ZarrCore generic are always written as `ZarrCore.f(...)` (or
# `V3Codecs.f(...)`) below: writing a bare `f(...)` definition would silently
# create a new `ZarrBlosc.f` that shadows the generic instead of adding a
# method to it, and nothing in ZarrCore would ever see it.
import ZarrCore
using ZarrCore: Compressor
using ZarrCore.Codecs: V3Codecs
using ZarrCore.Codecs.V3Codecs: V3Codec

# ## Zarr v2: `BloscCompressor`

struct BloscCompressor <: Compressor
    blocksize::Int
    clevel::Int
    cname::String
    shuffle::Int
end

"""
    BloscCompressor(;blocksize=0, clevel=5, cname="lz4", shuffle=1)

Returns a `BloscCompressor` struct that can serve as a Zarr array compressor. Keyword arguments are:

* `clevel=5` the compression level, number between 0 (no compression) and 9 (max compression)
* `cname="lz4"` compressor name, can be one of `"blosclz"`, `"lz4"`, and `"lz4hc"`
* `shuffle=1` Either NOSHUFFLE (0), SHUFFLE (1), BITSHUFFLE (2) or AUTOSHUFFLE (-1).
    If AUTOSHUFFLE, bit-shuffle will be used for buffers with itemsize 1, and byte-shuffle will be used otherwise. The default is SHUFFLE.
"""
BloscCompressor(;blocksize=0, clevel=5, cname="lz4", shuffle=1) =
    BloscCompressor(blocksize, clevel, cname, shuffle)

function ZarrCore.getCompressor(::Type{BloscCompressor}, d::Dict)
    BloscCompressor(d["blocksize"], d["clevel"], d["cname"], d["shuffle"])
end

ZarrCore.zuncompress(a, ::BloscCompressor, T) = Blosc.decompress(Base.nonmissingtype(T), a)

function ZarrCore.zuncompress!(data::DenseArray, compressed, ::BloscCompressor)
    Blosc.decompress!(vec(data),compressed)
end


function ZarrCore.zcompress(a, c::BloscCompressor)
    itemsize = sizeof(eltype(a))
    shuffle = c.shuffle
    # Weird auto shuffle logic from
    # https://github.com/zarr-developers/numcodecs/blob/7d8f9762b4f0f9b5e135688b2eeb3f783f90f208/numcodecs/blosc.pyx#L264-L272
    if shuffle == -1
        if itemsize == 1
            shuffle = Blosc.BITSHUFFLE
        else
            shuffle = Blosc.SHUFFLE
        end
    elseif shuffle ∉ (Blosc.NOSHUFFLE, Blosc.SHUFFLE, Blosc.BITSHUFFLE)
        throw(ArgumentError("invalid shuffle argument; expected -1, 0, 1 or 2, found $shuffle"))
    end
    Blosc.set_compressor(c.cname)
    Blosc.compress(a; level=c.clevel, shuffle=shuffle)
end

JSON.lower(c::BloscCompressor) = Dict("id"=>"blosc", "cname"=>c.cname,
    "clevel"=>c.clevel, "shuffle"=>c.shuffle, "blocksize"=>c.blocksize)

function ZarrCore.v2_to_v3_codecs(c::BloscCompressor, typesize::Int)
    (BloscV3Codec(c.cname, c.clevel, c.shuffle, c.blocksize, typesize),)
end

# ## Zarr v3: `BloscV3Codec`

"""
    BloscV3Codec(cname, clevel, shuffle, blocksize, typesize)

The zarr v3 `blosc` bytes->bytes codec. `shuffle` is stored as an integer
(0 = noshuffle, 1 = shuffle, 2 = bitshuffle) and (de)serialised to the spec's
string spelling.
"""
struct BloscV3Codec <: V3Codec{:bytes, :bytes}
    cname::String
    clevel::Int
    shuffle::Int
    blocksize::Int
    typesize::Int
end
BloscV3Codec() = BloscV3Codec("lz4", 5, 1, 0, 4)
V3Codecs.name(::BloscV3Codec) = "blosc"

function JSON.lower(c::BloscV3Codec)
    shuffle_str = c.shuffle == 0 ? "noshuffle" :
                  c.shuffle == 1 ? "shuffle" :
                  c.shuffle == 2 ? "bitshuffle" :
                  throw(ArgumentError("Unknown shuffle integer: $(c.shuffle)"))
    Dict("name" => "blosc", "configuration" => Dict(
        "cname"     => c.cname,
        "clevel"    => c.clevel,
        "shuffle"   => shuffle_str,
        "blocksize" => c.blocksize,
        "typesize"  => c.typesize
    ))
end

function V3Codecs.codec_encode(c::BloscV3Codec, data::Vector{UInt8})
    comp = BloscCompressor(blocksize=c.blocksize, clevel=c.clevel, cname=c.cname, shuffle=c.shuffle)
    return ZarrCore.zcompress(data, comp)
end

function V3Codecs.codec_decode(c::BloscV3Codec, encoded::Vector{UInt8})
    comp = BloscCompressor(blocksize=c.blocksize, clevel=c.clevel, cname=c.cname, shuffle=c.shuffle)
    return collect(ZarrCore.zuncompress(encoded, comp, UInt8))
end

# Both registries live in `ZarrCore`, so the entries have to be added at *load*
# time, not at precompile time: a mutation of another package's global state
# made while this module's body runs is discarded when the precompiled image is
# written out, and the entry would simply be missing in every fresh session.
function __init__()
    ZarrCore.compressortypes["blosc"] = BloscCompressor
    V3Codecs.register_codec("blosc", BloscV3Codec) do config, ctx
        cname = get(config, "cname", "lz4")
        clevel = get(config, "clevel", 5)
        shuffle_val = get(config, "shuffle", "noshuffle")
        shuffle_int = shuffle_val isa Integer ? shuffle_val :
                      shuffle_val == "noshuffle"  ? 0 :
                      shuffle_val == "shuffle"     ? 1 :
                      shuffle_val == "bitshuffle"  ? 2 :
                      throw(ArgumentError("Unknown shuffle: \"$shuffle_val\"."))
        blocksize = get(config, "blocksize", 0)
        typesize_default = isnothing(ctx) ? 4 : ctx.elsize
        typesize = get(config, "typesize", typesize_default)
        BloscV3Codec(string(cname), clevel, shuffle_int, blocksize, typesize)
    end
end

@static if VERSION >= v"1.11"
    include("public_names_blosc.jl")
end

end # module
