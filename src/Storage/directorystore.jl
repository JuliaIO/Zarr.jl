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
  fname=joinpath(d.folder,i)
  write(fname,v)
  v
end

getsub(s::DirectoryStore, d::String) = DirectoryStore(joinpath(s.folder,d))

function newsub(s::DirectoryStore, d::String)
  p = mkpath(joinpath(s.folder,d))
  DirectoryStore(p)
end

storagesize(d::DirectoryStore) = sum(filter(i->i ∉ (".zattrs",".zarray"),readdir(d.folder))) do f
  filesize(joinpath(d.folder,f))
end

zname(s::DirectoryStore) = splitdir(s.folder)[2]

subdirs(s::DirectoryStore) = filter(i -> isdir(joinpath(s.folder, i)), readdir(s.folder))
Base.keys(s::DirectoryStore) = filter(i -> isfile(joinpath(s.folder, i)), readdir(s.folder))

path(s::DirectoryStore) = s.folder
