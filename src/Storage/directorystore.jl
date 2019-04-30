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
    DirectoryStore(p) = new(normalize_path(p))
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

"""
Creates a new DirectoryStore from given metadata by creating a folder on disk and writing the
.zarray and .zattrs files.
"""
function DirectoryStore(path,name,metadata,attrs)
  if isempty(name)
    name = splitdir(path)[2]
  else
    path = joinpath(path, name)
  end
  if isdir(path)
    !isempty(readdir(path)) && throw(ArgumentError("Directory $path is not empty"))
  else
    mkpath(path)
  end
  open(joinpath(path, ".zarray"), "w") do f
    JSON.print(f, metadata)
  end
  open(joinpath(path, ".zattrs"), "w") do f
    JSON.print(f, attrs)
  end
  DirectoryStore(path)
end

function DirectoryStore(s::DirectoryStore, d::String)
    DirectoryStore(joinpath(s.folder), d)
end

storagesize(d::DirectoryStore) = sum(filter(i->i ∉ (".zattrs",".zarray"),readdir(d.folder))) do f
  filesize(joinpath(d.folder,f))
end

isinitialized(s::DirectoryStore, i::String) = isfile(joinpath(s.folder, i))

zname(s::DirectoryStore) = splitdir(s.folder)[2]

is_zgroup(s::DirectoryStore) = isfile(joinpath(s.folder, ".zgroup"))
is_zarray(s::DirectoryStore) = isfile(joinpath(s.folder, ".zarray"))

subs(s::DirectoryStore) = filter(i -> isdir(joinpath(s.folder, i)), readdir(s.folder))

path(s::DirectoryStore) = s.folder
