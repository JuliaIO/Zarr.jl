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

# The numcodecs id that `BloscCompressor` is stored under in a v2 `.zarray` document.
ZarrCore.codec_id(::Type{BloscCompressor}) = "blosc"

# Statically typed counterpart of the `Dict` method above, for the juliac
# `--trim=safe` open path (`zopen(::Type{T}, ::Val{N}, ...)`). Absent keys fall
# back to the `BloscCompressor(;...)` keyword defaults (the `Dict` method has
# no defaults because a stored blosc configuration always carries all four).
# Both branches of every optional key return the same concrete type.
function ZarrCore.getCompressor(::Type{BloscCompressor}, c::ZarrCore.CompressorJSON)
    c.id == ZarrCore.codec_id(BloscCompressor) || throw(ArgumentError(string(
        "expected a \"blosc\" compressor in the stored metadata, got \"", c.id, "\"")))
    blocksize = 0
    if c.blocksize !== nothing
        blocksize = c.blocksize::Int
    end
    clevel = 5
    if c.clevel !== nothing
        clevel = c.clevel::Int
    end
    cname = "lz4"
    if c.cname !== nothing
        cname = c.cname::String
    end
    shuffle = 1
    if c.shuffle !== nothing
        shuffle = c.shuffle::Int
    end
    return BloscCompressor(blocksize, clevel, cname, shuffle)
end

ZarrCore.zuncompress(a, ::BloscCompressor, ::Type{T}) where {T} = Blosc.decompress(Base.nonmissingtype(T), a)

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

# NamedTuple (not `Dict`) so that serialising array metadata stays statically
# typed - see `ZarrCore.print_metadata`. The emitted JSON is unchanged.
JSON.lower(c::BloscCompressor) = (; id = "blosc", cname = c.cname,
    clevel = c.clevel, shuffle = c.shuffle, blocksize = c.blocksize)

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

# The v3 spec name that `BloscV3Codec` is stored under in a `zarr.json` document.
V3Codecs.codec_name(::Type{BloscV3Codec}) = "blosc"

# Statically typed counterpart of the `register_codec` parser in `__init__`, for
# the juliac `--trim=safe` v3 open path
# (`zopen(::Type{T}, ::Val{N}, ...; pipeline = ...)`). The defaults are exactly
# the `Dict` parser's, so a typed open of a document with keys missing yields the
# same codec as the dynamic one; every branch of an absent optional key returns
# the same concrete type.
function V3Codecs.getCodec(::Type{BloscV3Codec}, c::ZarrCore.CodecJSON, ctx)
    V3Codecs._check_codec_name(BloscV3Codec, c)
    cname = "lz4"
    clevel = 5
    shuffle = 0
    blocksize = 0
    typesize = ctx.elsize
    cfg = c.configuration
    if cfg !== nothing
        cc = cfg::ZarrCore.CodecConfigJSON
        cn = cc.cname
        if cn !== nothing
            cname = cn::String
        end
        cl = cc.clevel
        if cl !== nothing
            clevel = cl::Int
        end
        sh = cc.shuffle
        if sh !== nothing
            shs = sh::String
            if shs == "noshuffle"
                shuffle = 0
            elseif shs == "shuffle"
                shuffle = 1
            elseif shs == "bitshuffle"
                shuffle = 2
            else
                throw(ArgumentError(string("Unknown shuffle: \"", shs, "\".")))
            end
        end
        bs = cc.blocksize
        if bs !== nothing
            blocksize = bs::Int
        end
        ts = cc.typesize
        if ts !== nothing
            typesize = ts::Int
        end
    end
    return BloscV3Codec(cname, clevel, shuffle, blocksize, typesize)
end

# NamedTuple (not `Dict`) so that lowering v3 array metadata stays statically
# typed - see `ZarrCore.print_metadata`. The emitted JSON is unchanged.
function JSON.lower(c::BloscV3Codec)
    local shuffle_str::String
    if c.shuffle == 0
        shuffle_str = "noshuffle"
    elseif c.shuffle == 1
        shuffle_str = "shuffle"
    elseif c.shuffle == 2
        shuffle_str = "bitshuffle"
    else
        throw(ArgumentError("Unknown shuffle integer: $(c.shuffle)"))
    end
    return (; name = "blosc", configuration = (;
        cname     = c.cname,
        clevel    = c.clevel,
        shuffle   = shuffle_str,
        blocksize = c.blocksize,
        typesize  = c.typesize
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
