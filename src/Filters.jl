import JSON


abstract type Filter{T,TENC} end
function getfilters(d::Dict) 
    if !haskey(d,"filters")
        return nothing
    else
        if d["filters"] === nothing || isempty(d["filters"])
            return nothing
        end
        f = map(d["filters"]) do f
            getfilter(filterdict[f["id"]], f)
        end
        return (f...,)
    end
end
sourcetype(::Filter{T}) where T = T
desttype(::Filter{<:Any,T}) where T = T

zencode(ain,::Nothing) = ain

"""
    VLenArrayFilter(T)

Encodes and decodes variable-length arrays of arbitrary data type 
"""
struct VLenArrayFilter{T} <: Filter{T,UInt8} end

function zdecode(ain, ::VLenArrayFilter{T}) where T
    f = IOBuffer(ain)
    nitems = read(f, UInt32)
    out = Array{Vector{T}}(undef,nitems)
    for i=1:nitems
        len1 = read(f,UInt32)
        out[i] = read!(f,Array{T}(undef,len1 ÷ sizeof(T)))
    end
    close(f)
    out
end

#Encodes Array of Vectors a into bytes
function zencode(ain,::VLenArrayFilter)
    b = IOBuffer()
    nitems = length(ain)
    write(b,UInt32(nitems))
    for a in ain
        write(b, UInt32(length(a) * sizeof(eltype(a))))
        write(b, a)
    end
    take!(b)
end

JSON.lower(::VLenArrayFilter{T}) where T = Dict("id"=>"vlen-array","dtype"=> typestr(eltype(T)) )

getfilter(::Type{<:VLenArrayFilter}, f) = VLenArrayFilter{Vector{typestr(f["dtype"])}}()

"""
    VLenUTF8Filter

Encodes and decodes variable-length arrays of arbitrary data type 
"""
struct VLenUTF8Filter <: Filter{String,UInt8} end

function zdecode(ain, ::VLenUTF8Filter)
    arbuf = UInt8[]
    f = IOBuffer(ain)
    nitems = read(f, UInt32)
    out = Array{String}(undef,nitems)
    for i=1:nitems
        len1 = read(f,UInt32)
        resize!(arbuf,len1)
        read!(f,arbuf)
        out[i] = String(arbuf)
    end
    close(f)
    out
end

#Encodes Array of Vectors a into bytes
function zencode(ain,::VLenUTF8Filter)
    b = IOBuffer()
    nitems = length(ain)
    write(b,UInt32(nitems))
    for a in ain
        write(b, UInt32(sizeof(a)))
        write(b, a)
    end
    take!(b)
end

JSON.lower(::VLenUTF8Filter) = Dict("id"=>"vlen-utf8","dtype"=> "|O" )

getfilter(::Type{<:VLenUTF8Filter}, f) = VLenUTF8Filter()

filterdict = Dict("vlen-array"=>VLenArrayFilter, "vlen-utf8"=>VLenUTF8Filter)