"""
    ZarrBlosc

Blosc v2 compressor and v3 codec support.
"""
module ZarrBlosc

import Blosc
import JSON # for JSON.lower

# Qualify methods that extend ZarrCore or V3Codecs generics.
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

ZarrCore.default_compressor() = BloscCompressor()

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
    # Match numcodecs AUTOSHUFFLE behavior.
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

# Cross-package registrations must run after precompilation.
function __init__()
    ZarrCore.should_register_at_init() && register!()
end

"""
    ZarrBlosc.register!()

Register the Blosc compressor with ZarrCore under the Zarr v2 compressor name
`"blosc"` and the Zarr v3 codec name `"blosc"`.

Registration runs automatically during package initialization when the
ZarrCore `RegisterAtInit` preference is enabled (the default). When automatic
registration is disabled, call this function explicitly. Calling it again
restores or overwrites ZarrBlosc's own registry entries.
"""
function register!()
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
