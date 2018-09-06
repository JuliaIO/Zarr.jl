module Compressors
import Blosc

abstract type Compressor end
struct BloscCompressor <: Compressor
    blocksize::Int
    clevel::Int
    cname::String
    shuffle::Bool
end
function getCompressor(::Type{BloscCompressor},d::Dict)
    BloscCompressor(d["blocksize"],d["clevel"],d["cname"],d["shuffle"]>0)
end
function read_uncompress!(a,f::String,c::BloscCompressor)
  r=read(f)
  read_uncompress!(a,r,c)
end
read_uncompress!(a,r::AbstractArray,::BloscCompressor)=Blosc.decompress!(reshape(a,length(a)),r);
function write_compress(a,f::String,c::BloscCompressor)
  Blosc.set_compressor(c.cname)
  r=Blosc.compress(a; level=c.clevel, shuffle=c.shuffle)
  write(f,a)
end
function write_compress(a,f::AbstractArray,c::BloscCompressor)
  Blosc.set_compressor(c.cname)
  Blosc.compress!(f,a,level=c.clevel, shuffle=c.shuffle)
end

struct NoCompressor <: Compressor end


compressortypes = Dict("blosc"=>BloscCompressor, nothing=>NoCompressor)


read_uncompress!(a,f::String,::NoCompressor) = read!(f,a)
read_uncompress!(a,r::AbstractArray, ::NoCompressor) = copyto!(a,r)
write_compress(a,f::String,::NoCompressor)=write(f,a)
write_compress(a,f::AbstractArray,::NoCompressor)=copyto!(f,a)

end
