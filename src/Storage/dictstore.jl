# Stores data in a simple dict in memory
struct DictStore{T} <: AbstractStore
    name::String
    a::T
    attrs::Dict
end
function DictStore(path,name,metadata,attrs)
  nsubs = map((s, c) -> ceil(Int, s/c), metadata.shape, metadata.chunks)
  et = areltype(metadata.compressor, eltype(metadata))
  T=eltype(metadata)
  isempty(name) && (name="data")
  a = Array{et}(undef, nsubs...)
  for i in eachindex(a)
    a[i] = T[]
  end
  DictStore(name, a, attrs)
end
Base.show(io::IO,d::DictStore) = print(io,"Dictionary Storage")


storagesize(d::DictStore) = sum(sizeof,values(d.a))
zname(s::DictStore) = s.name

Base.getindex(d::DictStore,i::CartesianIndex) = d.a[i]
Base.getindex(d::DictStore,s::String) = error("not implemented for DictStore")
Base.setindex!(d::DictStore,v,i::CartesianIndex) = d.a[i] = v


"Checks if a chunk is initialized"
isinitialized(s::DictStore, i::CartesianIndex) = true

# change when DictStore ZGroups are implemented
is_zgroup(s::DictStore) = false
is_zarray(s::DictStore) = true
