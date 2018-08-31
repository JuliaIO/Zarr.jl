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
function read_uncompress!(a,f,::BloscCompressor)
  r=read(f)
  Blosc.decompress!(reshape(a,length(a)),r);
end


struct NoCompressor <: Compressor end


compressortypes = Dict("blosc"=>BloscCompressor, nothing=>NoCompressor)


function read_uncompress!(a,f,::NoCompressor)
  read!(f,a)
end


end
