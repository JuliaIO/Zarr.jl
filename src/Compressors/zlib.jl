#=
# Zlib compression

This file implements a Zlib compressor via ChunkCodecLibZlib.jl.

=#

using ChunkCodecLibZlib: ZlibEncodeOptions, encode, decode, ChunkCodecCore

"""
    ZlibCompressor(clevel=-1)
Returns a `ZlibCompressor` struct that can serve as a Zarr array compressor. Keyword arguments are:
* `clevel=-1` the compression level, number between -1 (Default), 0 (no compression) and 9 (max compression)
*  default is -1 compromise between speed and compression (currently equivalent to level 6).
"""
struct ZlibCompressor <: Compressor
    config::ZlibEncodeOptions
end

ZlibCompressor(clevel::Integer) = ZlibCompressor(ZlibEncodeOptions(;level=clevel))

ZlibCompressor(;clevel=-1) = ZlibCompressor(clevel)

function getCompressor(::Type{ZlibCompressor}, d::Dict)
    ZlibCompressor(d["level"])
end

function zuncompress(a, z::ZlibCompressor, T)
    result = decode(z.config.codec, a)
    _reinterpret(Base.nonmissingtype(T),result)
end

function zuncompress!(data::DenseArray, compressed, z::ZlibCompressor)
    dst = reinterpret(UInt8, vec(data))
    n = length(dst)
    n_decoded = something(ChunkCodecCore.try_decode!(z.config.codec, dst, compressed))::Int64
    n_decoded == n || error("expected to decode $n bytes, only got $n_decoded bytes")
    data
end

function zcompress(a, z::ZlibCompressor)
    encode(z.config, reinterpret(UInt8, vec(a)))
end

JSON.lower(z::ZlibCompressor) = Dict("id"=>"zlib", "level" => z.config.level)

Zarr.compressortypes["zlib"] = ZlibCompressor