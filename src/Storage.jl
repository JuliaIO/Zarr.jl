module Storage
abstract type ZStorage end
import JSON

struct DiskStorage <: ZStorage
  folder::String
end
getattrs(p::DiskStorage)=isfile(joinpath(p.folder,".zattrs")) ? JSON.parsefile(joinpath(p.folder,".zattrs")) : Dict()
function getchunk(s::DiskStorage, i::CartesianIndex)
  f = joinpath(s.folder,join(reverse((i-one(i)).I),'.'))
  if !isfile(f)
    open(f,"w") do _
      nothing
    end
  end
  f
end
zname(z::DiskStorage)=splitdir(z.folder)[2]

struct MemStorage{T} <: ZStorage
  name::String
  a::T
end
zname(s::MemStorage)=s.name

"Returns the chunk at index i if present"
function getchunk(s::MemStorage,  i::CartesianIndex)
  s.a[i]
end

end
