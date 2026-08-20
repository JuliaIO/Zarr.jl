#=
# Shuffle compression

This file implements the shuffle compressor.
=#

struct ShuffleFilter <: Filter{UInt8, UInt8}
    elementsize::Csize_t
end

ShuffleFilter(; elementsize = 4) = ShuffleFilter(elementsize)

function _do_shuffle!(dest::AbstractVector{UInt8}, source::AbstractVector{UInt8}, elementsize::Csize_t)
    count = fld(length(source), elementsize) # elementsize is in bytes, so this works
    for i in 0:(count-1)
        offset = i * elementsize
        for byte_index in 0:(elementsize-1)
            j = byte_index * count + i
            dest[j+1] = source[offset + byte_index+1]
        end
    end
end

function _do_unshuffle!(dest::AbstractVector{UInt8}, source::AbstractVector{UInt8}, elementsize::Csize_t)
    count = fld(length(source), elementsize) # elementsize is in bytes, so this works
    for i in 0:(elementsize-1)
        offset = i * count
        for byte_index in 0:(count-1)
            j = byte_index * elementsize + i
            dest[j+1] = source[offset + byte_index+1]
        end
    end
end

function zencode(a::AbstractArray, c::ShuffleFilter)
    if c.elementsize <= 1 # no shuffling needed if elementsize is 1
        return a
    end
    source = reinterpret(UInt8, vec(a))
    dest = Vector{UInt8}(undef, length(source))
    _do_shuffle!(dest, source, c.elementsize)
    return dest
end

function zdecode(a::AbstractArray, c::ShuffleFilter)
    if c.elementsize <= 1 # no shuffling needed if elementsize is 1
        return a
    end
    source = reinterpret(UInt8, vec(a))
    dest = Vector{UInt8}(undef, length(source))
    _do_unshuffle!(dest, source, c.elementsize)
    return dest
end

function getfilter(::Type{ShuffleFilter}, d::Dict)
    return ShuffleFilter(d["elementsize"])
end

function JSON.lower(c::ShuffleFilter)
    return Dict("id" => "shuffle", "elementsize" => Int64(c.elementsize))
end

filterdict["shuffle"] = ShuffleFilter
#=

# Tests


    
=#