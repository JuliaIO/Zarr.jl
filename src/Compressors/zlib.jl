#=
# Zlib compression

This file implements a Zlib compressor via CodecZlib.jl.

=#

import CodecZlib

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
    _reinterpret(Base.nonmissingtype(T),result)
end

function zcompress(a, ::ZlibCompressor)
    a_uint8 = _reinterpret(UInt8,a)[:]
    transcode(CodecZlib.ZlibCompressor, a_uint8)
end

JSON.lower(z::ZlibCompressor) = Dict("id"=>"zlib", "level" => z.clevel)

Zarr.compressortypes["zlib"] = ZlibCompressor