import Blosc
import CodecZlib
import JSON


abstract type Compressor end
getCompressor(compdict::Dict) = getCompressor(compressortypes[compdict["id"]],compdict)
getCompressor(::Nothing) = NoCompressor()

#Compression when no filter is given
zcompress!(compressed,data,c,::Nothing) = zcompress!(compressed,data,c)
zuncompress!(data,compressed,c,::Nothing) = zuncompress!(data,compressed,c)

#Fallback definition of mutating form of compress and uncompress
function zcompress!(compressed, data, c) 
    empty!(compressed)
    append!(compressed,zcompress(data, c))
end
zuncompress!(data, compressed, c) = copyto!(data, zuncompress(compressed, c, eltype(data)))


#Function given a filter stack
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

function getCompressor(::Type{BloscCompressor}, d::Dict)
    BloscCompressor(d["blocksize"], d["clevel"], d["cname"], d["shuffle"])
end

zuncompress(a, ::BloscCompressor, T) = Blosc.decompress(Base.nonmissingtype(T), a)

function zuncompress!(data::DenseArray, compressed, ::BloscCompressor) 
    Blosc.decompress!(vec(data),compressed)
    # if Int(pointer(data,length(data))-pointer(data)) != (length(data)-1)*sizeof(eltype(data))
    #     @show size(data)
    #     @show size(parent(data))
    #     @show typeof(data)
    #     @show Int(pointer(data,length(data))-pointer(data))
    #     @show (length(data)-1)*sizeof(eltype(data))
    #     error("Something is wrong")
    # end
    # Zarr.Blosc.blosc_decompress(data, compressed, sizeof(data))
end


function zcompress(a, c::BloscCompressor)
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

"""
    NoCompressor()

Creates an object that can be passed to ZArray constructors without compression.
"""
struct NoCompressor <: Compressor end

function zuncompress(a, ::NoCompressor, T)
  reinterpret(T,a)
end

function zcompress(a, ::NoCompressor)
  reinterpret(UInt8,a)
end

JSON.lower(::NoCompressor) = nothing

compressortypes = Dict("blosc"=>BloscCompressor, nothing=>NoCompressor)



"""
    ZlibCompressor(clevel=-1)
Returns a `ZlibCompressor` struct that can serve as a Zarr array compressor. Keyword arguments are:
* `clevel=-1` the compression level, number between -1 (Default), 0 (no compression) and 9 (max compression)
*  default is -1 compromise between speed and compression (currently equivalent to level 6).
"""
struct ZlibCompressor <: Compressor
    clevel::Int
end

ZlibCompressor(;clevel=-1) = ZlibCompressor(clevel)

function getCompressor(::Type{ZlibCompressor}, d::Dict)
    ZlibCompressor(d["level"])
end

function zuncompress(a, ::ZlibCompressor, T)
    result = transcode(CodecZlib.ZlibDecompressor,a)
    reinterpret(Base.nonmissingtype(T),result)
end

function zcompress(a, ::ZlibCompressor)
    a_uint8 = reinterpret(UInt8,a)[:]
    transcode(CodecZlib.ZlibCompressor, a_uint8)
end

JSON.lower(z::ZlibCompressor) = Dict("id"=>"zlib", "level" => z.clevel)

Zarr.compressortypes["zlib"] = ZlibCompressor
