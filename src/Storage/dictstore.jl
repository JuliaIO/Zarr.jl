# Stores data in a simple dict in memory
struct DictStore <: AbstractStore
  a::Dict{String,Vector{UInt8}}
end
DictStore() = DictStore(Dict{String,Vector{UInt8}}())

Base.show(io::IO,d::DictStore) = print(io,"Dictionary Storage")
function _pdict(d::DictStore,p) 
  p = (isempty(p) || endswith(p,'/')) ? p : p*'/'
  filter(((k,v),)->startswith(k,p),d.a)
end
function storagesize(d::DictStore,p) 
  sum(i->last(split(i[1],'/')) ∉ (".zattrs",".zarray") ? sizeof(i[2]) : zero(sizeof(i[2])), _pdict(d,p))
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
  _searchsubdict(d2,p,(sp,lp)->length(sp) > lp+1)
end

function subkeys(d::DictStore,p) 
  d2 = _pdict(d,p)
  _searchsubdict(d2,p,(sp,lp)->length(sp) == lp+1)
end

function _searchsubdict(d2,p,condition)
  o = Set{String}()
  pspl = split(rstrip(p,'/'),'/')
  lp = if length(pspl) == 1 && isempty(pspl[1])
    0
  else
    length(pspl)
  end
  for k in keys(d2)
    sp = split(k,'/')
    if condition(sp,lp)
      push!(o,sp[lp+1])
    end
  end
  collect(o)
end


#getsub(d::DictStore, p, n) = _substore(d,p).subdirs[n]

#path(d::DictStore) = ""
