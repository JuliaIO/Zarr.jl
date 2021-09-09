# Stores data in a simple dict in memory
struct DictStore <: AbstractStore
  a::Dict{String,Vector{UInt8}}
end
DictStore() = DictStore(Dict{String,Vector{UInt8}}())

Base.show(io::IO,d::DictStore) = print(io,"Dictionary Storage")
_pdict(d,p) = filter(((k,v),)->startswith(k,p),d.a)
function storagesize(d::DictStore,p) 
  sum(i->i[1] ∉ (".zattrs",".zarray") ? sizeof(i[2]) : zero(sizeof(i[2])), _pdict(d,p))
end

function Base.getindex(d::DictStore,i::AbstractString) 
  get(d.a,i,nothing)
end
function Base.setindex!(d::DictStore,v,i::AbstractString) 
  d.a[i] = v
end
Base.delete!(d::DictStore, i::AbstractString) = delete!(d.a,i)

function subdirs(d::DictStore,p) 
  d2 = _pdict(d,p)
  o = Set{String}()
  lp = length(split(rstrip(p,'/'),'/'))
  for (k,_) in d2
    sp = split(k,'/')
    if length(sp) > lp+1
      push!(o,sp[lp+1])
    end
  end
  collect(o)
end

function subkeys(d::DictStore,p) 
  d2 = _pdict(d,p)
  o = Set{String}()
  lp = length(split(rstrip(p,'/'),'/'))
  for (k,_) in d2
    sp = split(k,'/')
    if length(sp) == lp+1
      push!(o,sp[lp+1])
    end
  end
  collect(o)
end
#getsub(d::DictStore, p, n) = _substore(d,p).subdirs[n]

#path(d::DictStore) = ""
