"Normalize logical storage path"
function normalize_path(p::AbstractString)
    # \ to / since normpath on linux won't handle it
    p = replace(p, '\\'=>'/')
    p = normpath(p)
    # \ to / again since normpath on windows creates \
    p = replace(p, '\\'=>'/')
    p == "/" ? p : rstrip(p, '/')
end

# Stores files in a regular file system
struct DirectoryStore <: AbstractStore
    folder::String
    function DirectoryStore(p)
      mkpath(normalize_path(p))
      new(normalize_path(p))
    end
end

function Base.getindex(d::DirectoryStore, i::String)
  fname=joinpath(d.folder,i)
  if isfile(fname)
    read(fname)
  else
    nothing
  end
end

function Base.setindex!(d::DirectoryStore,v,i::String)
  fname=d.folder * "/" * i
  folder = dirname(fname)
  isdir(folder) || mkpath(folder)
  write(fname,v)
  v
end


function storagesize(d::DirectoryStore,p) 
    sum(f -> filesize(d.folder * "/" * p * "/" * f), filter(i->i ∉ (".zattrs",".zarray"),readdir(d.folder * "/" * p)); init=0)
end

function subdirs(s::DirectoryStore,p) 
  pbase = joinpath(s.folder,p)
  if !isdir(pbase) 
    return String[]
  else
    return filter(i -> isdir(joinpath(s.folder,p, i)), readdir(pbase))
  end
end
function subkeys(s::DirectoryStore,p) 
  pbase = joinpath(s.folder,p)
  if !isdir(pbase) 
    return String[]
  else
    return filter(i -> isfile(joinpath(s.folder,p, i)), readdir(pbase))
  end
end
Base.delete!(s::DirectoryStore, k::String) = isfile(joinpath(s.folder, k)) && rm(joinpath(s.folder, k))

# Partial-read support. seek+read into a fresh buffer is much cheaper
# than reading the whole file when the caller only wants a few KB.
supports_partial_reads(::DirectoryStore) = true

function read_range(d::DirectoryStore, i::AbstractString, byte_range::UnitRange{Int})
    fname = joinpath(d.folder, i)
    isfile(fname) || return nothing
    n = length(byte_range)
    n == 0 && return UInt8[]
    buf = Vector{UInt8}(undef, n)
    open(fname, "r") do io
        seek(io, first(byte_range) - 1)
        readbytes!(io, buf, n)
    end
    return buf
end

function getsize(d::DirectoryStore, i::AbstractString)
    fname = joinpath(d.folder, i)
    isfile(fname) ? filesize(fname) : 0
end

