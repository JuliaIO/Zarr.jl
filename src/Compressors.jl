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

function read_uncompress!(a, f::String, c::BloscCompressor, s::ST) where ST <: AbstractStore
    if s isa DirectoryStore
        r = read(f)
    elseif s isa S3Store
        r = readobject(f, s)
    else
        throw(ArgumentError("Unknown type of storage."))
    end
    length(r) > 0 && read_uncompress!(a, r, c)
end

read_uncompress!(a, r::AbstractArray, ::BloscCompressor) = copyto!(a, Blosc.decompress(eltype(a), r))

function write_compress(a, f::String, c::BloscCompressor)
    Blosc.set_compressor(c.cname)
    r = Blosc.compress(a; level=c.clevel, shuffle=c.shuffle)
    write(f, r)
end

function write_compress(a, f::AbstractArray, c::BloscCompressor)
    Blosc.set_compressor(c.cname)
    r = Blosc.compress(a, level=c.clevel, shuffle=c.shuffle)
    empty!(f)
    append!(f, r)
end

areltype(::BloscCompressor, _) = Vector{UInt8}
JSON.lower(c::BloscCompressor) = Dict("id"=>"blosc", "cname"=>c.cname,
    "clevel"=>c.clevel, "shuffle"=>c.shuffle ? 1 : 0, "blocksize"=>c.blocksize)

"""
    NoCompressor()

Creates an object that can be passed to ZArray constructors without compression.
"""
struct NoCompressor <: Compressor end


compressortypes = Dict("blosc"=>BloscCompressor, nothing=>NoCompressor)

read_uncompress!(a, f::String, ::NoCompressor) = filesize(f) > 0 && read!(f, a)
read_uncompress!(a, r::AbstractArray, ::NoCompressor) = copyto!(a, r)
write_compress(a, f::String, ::NoCompressor) = write(f, a)

function write_compress(a, f::AbstractArray, ::NoCompressor)
    empty!(f)
    append!(f, a)
end

areltype(::NoCompressor,T) = Vector{T}
JSON.lower(::NoCompressor) = nothing
