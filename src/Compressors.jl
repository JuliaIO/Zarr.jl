import Blosc

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
