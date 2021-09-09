"""
A store that wraps any other AbstractStore but has access to the consolidated metadata
stored in the .zmetadata key. Whenever data attributes or metadata are accessed, the
data will be read from the dictionary instead.
"""
struct ConsolidatedStore{P} <: AbstractStore
  parent::P
  path::String
  cons::DictStore
end
function ConsolidatedStore(s::AbstractStore, p)
  d = s[p, ".zmetadata"]
  if d === nothing
    throw(ArgumentError("Could not find consolidated metadata for store $s"))
  end
  ConsolidatedStore(s,p,DictStore(JSON.parse(String(Zarr.maybecopy(d)))["metadata"]))
end

function Base.show(io::IO,d::ConsolidatedStore)
    b = IOBuffer()
    show(b,d.parent)
    print(io, "Consolidated ", String(take!(b)))
end

storagesize(d::ConsolidatedStore) = storagesize(d.parent,p)
function Base.getindex(d::ConsolidatedStore,i::String) 
  r = d.cons[i]
  if r === nothing
    d.parent[i]
  end
end
# getmetadata(d::ConsolidatedStore,p) = Metadata(d.cons[".zarray"])
# getattrs(d::ConsolidatedStore) = get(d.cons,".zattrs", Dict{String,Any}())
is_zarray(d::ConsolidatedStore,p) = is_zarray(d.cons,p)
is_zgroup(d::ConsolidatedStore,p) = is_zgroup(d.cons,p)
check_consolidated_write(i::String) = split(i,'/')[end] in (".zattrs",".zarray",".zgroup") &&
    throw(ArgumentError("Can not modify consolidated metadata, please re-open the dataset with `consolidated=false`"))
function Base.setindex!(d::ConsolidatedStore,v,i::String)
  #Here we check that we don't overwrite consolidated information
  check_consolidated_write(i)
  d.parent[i] = v
end
function Base.delete!(d::ConsolidatedStore,i::String)
  check_consolidated_write(i)
  delete!(d.parent,i)
end

function consolidate_metadata(s::AbstractStore,d,prefix)
  for k in (".zattrs",".zarray",".zgroup")
    v = s[prefix,k]
    if v !== nothing
      d[string(prefix,k)] = JSON.parse(replace(String(Zarr.maybecopy(v)),": NaN,"=>": \"NaN\","))
    end
  end
  foreach(subdirs(s)) do subname
    consolidate_metadata(s,d,string(prefix,subname,"/"))
  end
  d
end
function consolidate_metadata(s::AbstractStore)
  d = consolidate_metadata(s,Dict{String,Any}(),"")
  buf = IOBuffer()
  JSON.print(buf,Dict("metadata"=>d),4)
  s[".zmetadata"] = take!(buf)
  ConsolidatedStore(s)
end

consolidate_metadata(s) = consolidate_metadata(zopen(s))
