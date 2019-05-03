# Stores data in a simple dict in memory
struct DictStore <: AbstractStore
  name::String
  a::Dict{String,Vector{UInt8}}
  subdirs::Dict{String,DictStore}
end
function DictStore(name)
  isempty(name) && (name="data")
  a = Dict{String,Vector{UInt8}}()
  subdirs = Dict{String,DictStore}()
  DictStore(name, a, subdirs)
end
DictStore() = DictStore("")
Base.show(io::IO,d::DictStore) = print(io,"Dictionary Storage")

storagesize(d::DictStore) = sum(i->i[1] ∉ (".zattrs",".zarray") ? sizeof(i[2]) : zero(sizeof(i[2])),d.a)
zname(s::DictStore) = s.name

Base.getindex(d::DictStore,i::String) = get(d.a,i,nothing)
Base.setindex!(d::DictStore,v,i::String) = d.a[i] = v

subdirs(d::DictStore) = keys(d.subdirs)
Base.keys(d::DictStore) = keys(d.a)
newsub(d::DictStore, n) = d.subdirs[n] = DictStore(n)
getsub(d::DictStore, n) = d.subdirs[n]

path(d::DictStore) = ""
