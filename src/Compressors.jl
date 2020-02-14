import Blosc
import CodecZlib
import JSON


abstract type Compressor end
getCompressor(compdict::Dict) = getCompressor(compressortypes[compdict["id"]],compdict)
getCompressor(::Nothing) = NoCompressor()


struct BloscCompressor <: Compressor
    blocksize::Int
    clevel::Int
    cname::String
    shuffle::Bool
end

"""
    BloscCompressor(;blocksize=0, clevel=5, cname="lz4", shuffle=true)

Returns a `BloscCompressor` struct that can serve as a Zarr array compressor. Keyword arguments are:

* `clevel=5` the compression level, number between 0 (no compression) and 9 (max compression)
* `cname="lz4"` compressor name, can be one of `"blosclz"`, `"lz4"`, and `"lz4hc"`
* `shuffle=true` enables/disables bit-shuffling
"""
BloscCompressor(;blocksize=0, clevel=5, cname="lz4", shuffle=true) =
    BloscCompressor(blocksize, clevel, cname, shuffle)

function getCompressor(::Type{BloscCompressor}, d::Dict)
    BloscCompressor(d["blocksize"], d["clevel"], d["cname"], d["shuffle"] > 0)
end

zuncompress(a, r::AbstractArray, ::BloscCompressor) = copyto!(a, Blosc.decompress(Base.nonmissingtype(eltype(a)), r))

function zcompress(a, f::AbstractArray, c::BloscCompressor)
    Blosc.set_compressor(c.cname)
    r = Blosc.compress(a, level=c.clevel, shuffle=c.shuffle)
    empty!(f)
    append!(f, r)
end

JSON.lower(c::BloscCompressor) = Dict("id"=>"blosc", "cname"=>c.cname,
    "clevel"=>c.clevel, "shuffle"=>c.shuffle ? 1 : 0, "blocksize"=>c.blocksize)

"""
    NoCompressor()

Creates an object that can be passed to ZArray constructors without compression.
"""
struct NoCompressor <: Compressor end

function zuncompress(a, r::AbstractArray, ::NoCompressor)
  copyto!(a, reinterpret(eltype(a),r))
end

function zcompress(a, f::AbstractArray, ::NoCompressor)
  a2 = reinterpret(UInt8,a)
  empty!(f)
  append!(f, a2)
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

function zuncompress(a, r::AbstractArray, ::ZlibCompressor)
    result = transcode(CodecZlib.ZlibDecompressor,r)
    copyto!(a, reinterpret(Base.nonmissingtype(eltype(a)),result))
end

function zcompress(a, f::AbstractArray, ::ZlibCompressor)
    a_uint8 = reinterpret(UInt8,a)
    r = transcode(CodecZlib.ZlibCompressor,a_uint8[:])
    empty!(f)
    append!(f,r)
end

JSON.lower(z::ZlibCompressor) = Dict("id"=>"zlib", "level" => z.clevel)

Zarr.compressortypes["zlib"] = ZlibCompressor
