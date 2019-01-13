# Defines different storages for zarr arrays. Currently only regular files (DiskStorage)
# and Dictionaries are supported
import JSON

abstract type ZStorage end

"Normalize logical storage path"
function normalize_path(p::AbstractString)
    p = normpath(p)
    p = replace(p, '\\'=>'/')
    strip(p, '/')
end

# Stores files in a regular file system
struct DiskStorage <: ZStorage
    folder::String
    DiskStorage(p) = new(normalize_path(p))
end

function getattrs(s::DiskStorage)
    if isfile(joinpath(s.folder, ".zattrs"))
        JSON.parsefile(joinpath(s.folder, ".zattrs"))
    else
        Dict()
    end
end

function getchunk(s::DiskStorage, i::CartesianIndex)
    f = joinpath(s.folder, join(reverse((i - one(i)).I), '.'))
    if !isfile(f)
        open(f, "w") do _
           nothing
        end
    end
    f
end

function adddir(s::DiskStorage, i::String)
    f = joinpath(s.folder, i)
    mkpath(f)
end

zname(s::DiskStorage) = splitdir(s.folder)[2]


# Stores data in a simple dict in memory
struct MemStorage{T} <: ZStorage
    name::String
    a::T
end

zname(s::MemStorage) = s.name

"Returns the chunk at index i if present"
function getchunk(s::MemStorage,  i::CartesianIndex)
    s.a[i]
end
