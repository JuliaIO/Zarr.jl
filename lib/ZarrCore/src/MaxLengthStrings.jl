#This is a modified version of FixedSizeStrings.jl
#adapted so that appended zero Chars are omitted from
#the string. So most credits got to authors of
#https://github.com/JuliaComputing/FixedSizeStrings.jl
# Maybe this should be moved to FizedSizeStrings.jl,
#but until then let's keep it here...
module MaxLengthStrings
import Base: iterate, lastindex, getindex, sizeof, length, ncodeunits, codeunit, isvalid, read, write
export MaxLengthString

struct MaxLengthString{N,T} <: AbstractString
    data::NTuple{N,T}
    function MaxLengthString{N,T}(itr) where {N,T}
      new(totuple_appendzero(NTuple{N,T},itr))
    end
end
import Base: tuple_type_head, tuple_type_tail
#totuple_appendzero(::Type{Tuple{}}, itr, s...) = ()
tuple_append(::Type{NTuple{N,T}}) where {N,T} = (zero(T), tuple_append(NTuple{N-1,T})...)
tuple_append(::Type{Tuple{}}) = ()
function totuple_appendzero(::Type{Tuple{}},itr,s...)
  iterate(itr, s...)===nothing || error("String is too long to fit into MaxLengthString")
  ()
end
function totuple_appendzero(::Type{NTuple{N,T}}, itr, s...)::Tuple{Vararg{T,N}} where {N,T}
    y = iterate(itr, s...)
    if y === nothing
      tuple_append(NTuple{N,T})
    else
      (convert(T, y[1]), totuple_appendzero(NTuple{N-1,T}, itr, y[2])...)
    end
end

function MaxLengthString(s::AbstractString,N=length(s),T=UInt8)
  MaxLengthString{N,T}(rpad(s,N,'\0'))
end

function iterate(s::MaxLengthString{N}, i::Int = 1) where N
    i > N && return nothing
    c = s.data[i]
    iszero(c) && return nothing
    return (Char(c), i+1)
end

lastindex(s::MaxLengthString{N}) where {N} = findlast(!iszero,s.data)

function getindex(s::MaxLengthString, i::Int)
  checkbounds(s,i)
  Char(s.data[i])
end

sizeof(s::MaxLengthString) = sizeof(s.data)

length(s::MaxLengthString) = findlast(!iszero,s.data)

ncodeunits(s::MaxLengthString) = length(s)

codeunit(::MaxLengthString{<:Any,T}) where T = T
function codeunit(s::MaxLengthString, i::Integer)
  checkbounds(s,i)
  s.data[i]
end

isvalid(s::MaxLengthString{<:Any,UInt8}, i::Int) = checkbounds(Bool, s, i)
isvalid(s::MaxLengthString, i::Int) = checkbounds(Bool, s, i) && isvalid(Char,s.data[i])

function read(io::IO, T::Type{<:MaxLengthString{N}}) where N
    return read!(io, Ref{T}())[]::T
end

function write(io::IO, s::MaxLengthString{N}) where N
    return write(io, Ref(s))
end
end
