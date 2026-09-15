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
    # Explicit open/close instead of `read(fname)`: the latter routes through
    # `open(f, args...; kwargs...)`, whose `Core._apply_iterate` splat cannot be
    # statically resolved (juliac `--trim`).
    io = open(fname, "r")
    try
      read(io)
    finally
      close(io)
    end
  else
    nothing
  end
end

function Base.setindex!(d::DirectoryStore,v,i::String)
  fname = joinpath(d.folder, i)
  folder = dirname(fname)
  isdir(folder) || mkpath(folder)
  tmp = tempname(folder)
  try
    # Explicit open/close instead of `write(tmp, v)`: the latter routes through
    # `open(f, args...; kwargs...)`, whose `Core._apply_iterate` splat cannot be
    # statically resolved (juliac `--trim`).
    io = open(tmp, "w")
    try
      write(io, v)
    finally
      close(io)
    end
    mv(tmp, fname, force=true)  # atomic on POSIX
    return v
  catch
    rm(tmp, force=true)
    rethrow()
  end
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

