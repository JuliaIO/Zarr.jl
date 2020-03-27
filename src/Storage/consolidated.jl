"""
A store that wraps any other AbstractStore but has access to the consolidated metadata
stored in the .zmetadata key. Whenever data attributes or metadata are accessed, the
data will be read from the dictionary instead.
"""
struct ConsolidatedStore{P} <: AbstractStore
  parent::P
  cons::Dict{String}
end
function ConsolidatedStore(s::AbstractStore)
  d = s[".zmetadata"]
  if d === nothing
    throw(ArgumentError("Could not find consolidated metadata for store $s"))
  end
  ConsolidatedStore(s,JSON.parse(String(Zarr.maybecopy(d)))["metadata"])
end

function Base.show(io::IO,d::ConsolidatedStore)
    b = IOBuffer()
    show(b,d.parent)
    print(io, "Consolidated ", String(take!(b)))
end

storagesize(d::ConsolidatedStore) = storagesize(d.parent)
zname(s::ConsolidatedStore) = zname(s.parent)
Base.getindex(d::ConsolidatedStore,i::String) = d.parent[i]
getmetadata(d::ConsolidatedStore) = Metadata(d.cons[".zarray"])
getattrs(d::ConsolidatedStore) = get(d.cons,".zattrs", Dict{String,Any}())
is_zarray(d::ConsolidatedStore) = haskey(d.cons,".zarray")
is_zgroup(d::ConsolidatedStore) = haskey(d.cons,".zgroup")
check_consolidated_write(i::String) = split(i,'/')[end] in (".zattrs",".zarray",".zgroup") &&
    throw(ArgumentError("Can not modify consolidated metadata, please re-open the dataset with `consolidated=false`"))
function Base.setindex!(d::ConsolidatedStore,v,i::String)
  #Here we check that we don't overwrite consolidated information
  check_consolidated_write(i)
  d.parent.a[i] = v
end
function Base.delete!(d::ConsolidatedStore,i::String)
  check_consolidated_write(i)
  delete!(d.a,i)
end
function subdirs(d::ConsolidatedStore)
  o = Set{String}()
  foreach(collect(keys(d.cons))) do k
    tr = split(k,'/')
    length(tr) > 1 && push!(o,first(tr))
  end
  collect(o)
end
Base.keys(d::ConsolidatedStore) = keys(d.parent)
newsub(d::ConsolidatedStore, n) = newsub(d.parent, n)
function getsub(d::ConsolidatedStore, n)
    newd = Dict{String,Any}()
    for (k,v) in d.cons
        s = split(k,'/')
        if s[1] == n
            newd[join(s[2:end],'/')] = v
        end
    end
    ConsolidatedStore(getsub(d.parent, n), newd)
end
path(d::ConsolidatedStore) = path(d.parent)

function consolidate_metadata(s::AbstractStore,d,prefix)
  for k in (".zattrs",".zarray",".zgroup")
    v = s[k]
    if v !== nothing
      d[string(prefix,k)] = JSON.parse(replace(String(Zarr.maybecopy(v)),": NaN,"=>": \"NaN\","))
    end
  end
  foreach(subdirs(s)) do subname
    ssub = getsub(s,subname)
    consolidate_metadata(ssub,d,string(prefix,subname,"/"))
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
