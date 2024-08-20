#=
# Variable-length filters

This file implements variable-length filters for Zarr, i.e., filters that write arrays of variable-length arrays ("ragged arrays").

Specifically, it implements the `VLenArrayFilter` and `VLenUTF8Filter` types, which are used to encode and decode variable-length arrays and UTF-8 strings, respectively.
=#

# ## VLenArrayFilter

"""
    VLenArrayFilter(T)

Encodes and decodes variable-length arrays of arbitrary data type `T`.
"""
struct VLenArrayFilter{T} <: Filter{T,UInt8} end
# We don't need to define `sourcetype` and `desttype` for this filter, since the generic implementations are sufficient.

JSON.lower(::VLenArrayFilter{T}) where T = Dict("id"=>"vlen-array","dtype"=> typestr(T) )
getfilter(::Type{<:VLenArrayFilter}, f) = VLenArrayFilter{typestr(f["dtype"])}()
filterdict["vlen-array"] = VLenArrayFilter

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

#Encodes Array of Vectors `ain` into bytes
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

# ## VLenUTF8Filter

"""
    VLenUTF8Filter

Encodes and decodes variable-length unicode strings
"""
struct VLenUTF8Filter <: Filter{String, UInt8} end

JSON.lower(::VLenUTF8Filter) = Dict("id"=>"vlen-utf8")
getfilter(::Type{<:VLenUTF8Filter}, f) = VLenUTF8Filter()
filterdict["vlen-utf8"] = VLenUTF8Filter

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
