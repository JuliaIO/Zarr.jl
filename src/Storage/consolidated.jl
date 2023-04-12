"""
A store that wraps any other AbstractStore but has access to the consolidated metadata
stored in the .zmetadata key. Whenever data attributes or metadata are accessed, the
data will be read from the dictionary instead.
"""
struct ConsolidatedStore{P} <: AbstractStore
  parent::P
  path::String
  cons::Dict{String,Any}
end
function ConsolidatedStore(s::AbstractStore, p)
  d = s[p, ".zmetadata"]
  if d === nothing
    throw(ArgumentError("Could not find consolidated metadata for store $s"))
  end
  ConsolidatedStore(s,p,JSON.parse(String(Zarr.maybecopy(d)))["metadata"])
end

function Base.show(io::IO,d::ConsolidatedStore)
    b = IOBuffer()
    show(b,d.parent)
    print(io, "Consolidated ", String(take!(b)))
end

storagesize(d::ConsolidatedStore,p) = storagesize(d.parent,p)
function Base.getindex(d::ConsolidatedStore,i::String) 
    d.parent[i]
end
getmetadata(d::ConsolidatedStore,p,fill_as_missing) = Metadata(d.cons[_unconcpath(d,p,".zarray")],fill_as_missing)
getattrs(d::ConsolidatedStore, p) = get(d.cons,_unconcpath(d,p,".zattrs"), Dict{String,Any}())
function _unconcpath(d,p)
  startswith(p,d.path) || error("Requested key is not in consolidated path")
  lstrip(replace(p,d.path=>"", count=1),'/')
end
_unconcpath(d,p,s) = _concatpath(_unconcpath(d,p),s)
is_zarray(d::ConsolidatedStore,p) = haskey(d.cons,_unconcpath(d,p,".zarray"))
is_zgroup(d::ConsolidatedStore,p) = haskey(d.cons,_unconcpath(d,p,".zgroup"))
check_consolidated_write(i::String) = split(i,'/')[end] in (".zattrs",".zarray",".zgroup") &&
    throw(ArgumentError("Can not modify consolidated metadata, please re-open the dataset with `consolidated=false`"))

_pdict(d::ConsolidatedStore,p) = filter(((k,v),)->startswith(k,p),d.cons)
function subdirs(d::ConsolidatedStore,p) 
  p2 = _unconcpath(d,p)
  d2 = _pdict(d,p2)
  _searchsubdict(d2,p,(sp,lp)->length(sp) > lp+1)
end

function subkeys(d::ConsolidatedStore,p) 
  subkeys(d.parent,p)
end



function Base.setindex!(d::ConsolidatedStore,v,i::String)
  #Here we check that we don't overwrite consolidated information
  check_consolidated_write(i)
  d.parent[i] = v
end
function Base.delete!(d::ConsolidatedStore,i::String)
  check_consolidated_write(i)
  delete!(d.parent,i)
end

store_read_strategy(s::ConsolidatedStore) = store_read_strategy(s.parent)

function consolidate_metadata(s::AbstractStore,d,prefix)
  for k in (".zattrs",".zarray",".zgroup")
    v = s[prefix,k]
    if v !== nothing
      d[_concatpath(prefix,k)] = JSON.parse(String(copy(v)))
    end
  end
  foreach(subdirs(s,prefix)) do subname
    consolidate_metadata(s,d,string(prefix,subname,"/"))
  end
  d
end
function consolidate_metadata(s::AbstractStore,p)
  d = consolidate_metadata(s,Dict{String,Any}(),p)
  buf = IOBuffer()
  JSON.print(buf,Dict("metadata"=>d,"zarr_consolidated_format"=>1),4)
  s[p,".zmetadata"] = take!(buf)
  ConsolidatedStore(s,p,d)
end

consolidate_metadata(s) = consolidate_metadata(zopen(s,"w"))
