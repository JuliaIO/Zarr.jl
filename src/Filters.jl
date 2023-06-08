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

"""
    VLenUTF8Filter

Encodes and decodes variable-length unicode strings
"""
struct VLenUTF8Filter <: Filter{String, UInt8} end

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

function zdecode(ain, ::VLenUTF8Filter)
    f = IOBuffer(ain)
    nitems = read(f, UInt32)
    out = Array{String}(undef, nitems)
    for i in 1:nitems
        clen = read(f, UInt32)
        out[i] = String(read(f, clen))
    end
    close(f)
    out
end

function zencode(ain, ::VLenUTF8Filter)
    b = IOBuffer()
    nitems = length(ain)
    write(b, UInt32(nitems))
    for a in ain
        utf8encoded = transcode(String, a)
        write(b, UInt32(ncodeunits(utf8encoded)))
        write(b, utf8encoded)
    end
    take!(b)
end

JSON.lower(::VLenArrayFilter{T}) where T = Dict("id"=>"vlen-array","dtype"=> typestr(T) )
JSON.lower(::VLenUTF8Filter) = Dict("id"=>"vlen-utf8")

getfilter(::Type{<:VLenArrayFilter}, f) = VLenArrayFilter{typestr(f["dtype"])}()
getfilter(::Type{<:VLenUTF8Filter}, f) = VLenUTF8Filter()

filterdict = Dict("vlen-array"=>VLenArrayFilter, "vlen-utf8"=>VLenUTF8Filter)
