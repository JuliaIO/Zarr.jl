module Storage
abstract type ZStorage end
import JSON

struct DiskStorage <: ZStorage
  folder::String
end
getattrs(p::DiskStorage)=isfile(joinpath(p.folder,".zattrs")) ? JSON.parsefile(joinpath(p.folder,".zattrs")) : Dict()
getchunk(s::DiskStorage, i::CartesianIndex) = joinpath(s.folder,join(reverse(i.I),'.'))
zname(z::DiskStorage)=splitdir(z.folder)[2]

struct MemStorage{T} <: ZStorage
  name::String
  a::T
end
zname(s::MemStorage)=s.name
getchunk(s::MemStorage,  i::CartesianIndex) = s.a[i]

end
