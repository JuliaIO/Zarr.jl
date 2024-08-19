#=
# Blosc compression

This file implements a Blosc compressor via Blosc.jl.
=#

import Blosc

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

Zarr.compressortypes["blosc"] = BloscCompressor