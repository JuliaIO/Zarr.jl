module ZArray.jl
import .Compressors: Compressor, read_uncompress

ztype2jltype = Dict(
  "<f4"=>Float32,
  "<f8"=>Float64,
  "<i4"=>Int32,
  "<i8"=>Int64
)
zshape2shape(x) = ntuple(i->x[i],length(x))
struct C end
struct F end
const StorageOrder = Union{C,F}

zorder2order(x) = x=="C" ? C : x=="F" ? F : error("Unknown storage order")

getfillval(target::Type{T},t::String) where T<: Number =parse(T,t)
getfillval(target::Type{T},t::T) where T = t

struct ZArray{T,N,C<:Compressor}
    folder::String
    size::NTuple{N,Int}
    order::StorageOrder
    chunks::NTuple{N,Int}
    fillval::T
    compressor::C
end

function ZArray(folder::String)
    files = readdir(folder)
    @assert in(".zarray",files)
    arrayinfo = JSON.parsefile(joinpath(folder,".zarray"))
    dt = ztype2jltype[arrayinfo["dtype"]]
    shape = zshape2shape(arrayinfo["shape"])
    order = zorder2order(arrayinfo["order"])
    arrayinfo["zarr_format"] != 2 && error("Expecting Zarr format version 2")
    chunks = zshape2shape(arrayinfo["chunks"])
    fillval = getfillval(dt,arrayinfo["fill_value"])
    compdict = arrayinfo["compressor"]
    compressor = getCompressor(compressortypes[compdict["id"]],compdict)
    ZArray{dt,length(shape),typeof(compressor)}(folder,shape,order(),chunks,fillval,compressor)
end
function readchunk!(a::DenseArray{T},z::ZArray{T,N},i::CartesianIndex{N}) where {T,N}
    length(a)==prod(z.chunks) || throw(DimensionMismatch("Array size does not equal chunk size"))
    i=CartesianIndex((1,1,1))
    filename=joinpath(z.folder,join(map(i->i-1,i.I),'.'))
    a
end

end
