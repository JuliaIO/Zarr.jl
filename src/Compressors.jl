import Blosc

abstract type Compressor end
struct BloscCompressor <: Compressor
    blocksize::Int
    clevel::Int
    cname::String
    shuffle::Bool
end
BloscCompressor(;blocksize=0,clevel=5,cname="lz4",shuffle=true)=
  BloscCompressor(blocksize,clevel,cname,shuffle)
function getCompressor(::Type{BloscCompressor},d::Dict)
    BloscCompressor(d["blocksize"],d["clevel"],d["cname"],d["shuffle"]>0)
end
function read_uncompress!(a,f::String,c::BloscCompressor)
  r=read(f)
  read_uncompress!(a,r,c)
end
read_uncompress!(a,r::AbstractArray,::BloscCompressor)=copyto!(a,Blosc.decompress(eltype(a),r));

function write_compress(a,f::String,c::BloscCompressor)
  Blosc.set_compressor(c.cname)
  r=Blosc.compress(a; level=c.clevel, shuffle=c.shuffle)
  write(f,r)
end
function write_compress(a,f::AbstractArray,c::BloscCompressor)
  Blosc.set_compressor(c.cname)
  r = Blosc.compress(a,level=c.clevel, shuffle=c.shuffle)
  empty!(f)
  append!(f,r)
end
areltype(::BloscCompressor,_)=Vector{UInt8}
tojson(c::BloscCompressor)=Dict("id"=>"blosc","cname"=>c.cname,
  "clevel"=>c.clevel,"shuffle"=>c.shuffle ? 1 : 0, "blocksize"=>c.blocksize)

struct NoCompressor <: Compressor end


compressortypes = Dict("blosc"=>BloscCompressor, nothing=>NoCompressor)


read_uncompress!(a,f::String,::NoCompressor) = read!(f,a)
read_uncompress!(a,r::AbstractArray, ::NoCompressor) = copyto!(a,r)
write_compress(a,f::String,::NoCompressor)=write(f,a)
function write_compress(a,f::AbstractArray,::NoCompressor)
  empty!(f)
  append!(f,a)
end
areltype(::NoCompressor,T)=Vector{T}
tojson(::NoCompressor)=nothing
