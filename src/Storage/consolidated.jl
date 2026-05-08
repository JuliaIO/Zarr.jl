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
  # v2-style: separate .zmetadata file
  d = s[p, ".zmetadata"]
  if d !== nothing
      meta = JSON.parse(String(copy(d)); dicttype = Dict{String,Any})
      return ConsolidatedStore(s, p, meta["metadata"])
  end
  # v3-style: consolidated_metadata embedded inside root zarr.json
  z = s[p, "zarr.json"]
  if z !== nothing
      root = JSON.parse(String(copy(z)); dicttype = Dict{String,Any})
      cm = get(root, "consolidated_metadata", nothing)
      if cm !== nothing
          return ConsolidatedStore(s, p, cm["metadata"])
      end
  end
  throw(ArgumentError("Could not find consolidated metadata for store $s"))
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
function getmetadata(::ZarrFormat{2}, d::ConsolidatedStore, p, fill_as_missing)
    return Metadata(d.cons[_unconcpath(d, p, ".zarray")], fill_as_missing)
end
function getmetadata(::ZarrFormat{3}, d::ConsolidatedStore, p, fill_as_missing)
    return Metadata(d.cons[_unconcpath(d, p, "zarr.json")], fill_as_missing)
end
function getattrs(::ZarrFormat{2}, d::ConsolidatedStore, p)
  return get(d.cons, _unconcpath(d, p, ".zattrs"), Dict{String,Any}())
end

function getattrs(::ZarrFormat{3}, d::ConsolidatedStore, p)
  json_key = _unconcpath(d, p, "zarr.json")
  node_meta = get(d.cons, json_key, nothing)
  if node_meta === nothing
      return Dict{String,Any}()
  end
  # v3 spec: user attributes are stored under the "attributes" subkey of zarr.json
  if haskey(node_meta, "attributes")
      return node_meta["attributes"]
  end
  # fallback: return the full node metadata if no "attributes" key is found, we never write this. We only read it.
  # is this correct?
  return node_meta
end

function _unconcpath(d,p)
  startswith(p,d.path) || error("Requested key is not in consolidated path")
  lstrip(replace(p,d.path=>"", count=1),'/')
end
_unconcpath(d,p,s) = _concatpath(_unconcpath(d,p),s)
is_zarray(::ZarrFormat{2}, d::ConsolidatedStore, p) = haskey(d.cons, _unconcpath(d, p, ".zarray"))
is_zgroup(::ZarrFormat{2}, d::ConsolidatedStore, p) = haskey(d.cons, _unconcpath(d, p, ".zgroup"))
is_zarray(::ZarrFormat{3}, d::ConsolidatedStore, p) = haskey(d.cons, _unconcpath(d, p, "zarr.json")) && get(d.cons[_unconcpath(d, p, "zarr.json")], "node_type", "") == "array"
is_zgroup(::ZarrFormat{3}, d::ConsolidatedStore, p) = haskey(d.cons, _unconcpath(d, p, "zarr.json")) && get(d.cons[_unconcpath(d, p, "zarr.json")], "node_type", "") == "group"
ZarrFormat(d::ConsolidatedStore, path) = ZarrFormat(d.parent, path)  # detect format from parent, not cons
check_consolidated_write(i::String) = split(i,'/')[end] in (".zattrs",".zarray",".zgroup") &&
    throw(ArgumentError("Can not modify consolidated metadata, please re-open the dataset with `consolidated=false`"))

_pdict(d::ConsolidatedStore,p) = filter(((k,v),)->startswith(k,p),d.cons)
function subdirs(d::ConsolidatedStore,p) 
  p2 = _unconcpath(d,p)
  d2 = _pdict(d,p2)
  _searchsubdict(d2, p2, (sp, lp) -> length(sp) > lp + 1)
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
  # v2-style: separate .zmetadata file
  for k in (".zattrs",".zarray",".zgroup")
    v = s[prefix,k]
    if v !== nothing
      d[_concatpath(prefix,k)] = JSON.parse(String(copy(v)); dicttype = Dict{String,Any})
    end
  end
  # v3-style: consolidated_metadata embedded inside root zarr.json
  zj = s[prefix, "zarr.json"]
  if zj !== nothing
    d[_concatpath(prefix, "zarr.json")] = JSON.parse(String(copy(zj)); dicttype=Dict{String,Any})
  end
  foreach(subdirs(s,prefix)) do subname
    consolidate_metadata(s,d,string(prefix,subname,"/"))
  end
  d
end
function consolidate_metadata(s::AbstractStore,p)
  d = consolidate_metadata(s,Dict{String,Any}(),p)
  # detect format from root: v3 embeds into zarr.json, v2 writes separate .zmetadata
  zj = s[p, "zarr.json"]
  if zj !== nothing
      root = JSON.parse(String(copy(zj)); dicttype=Dict{String,Any})
      root["consolidated_metadata"] = Dict{String,Any}(
          "kind" => "inline",
          "must_understand" => false,
          "metadata" => d
      )
      buf = IOBuffer()
      JSON.print(buf, root, 4)
      s[p, "zarr.json"] = take!(buf)
  else
    # v2-style: separate .zmetadata file
    buf = IOBuffer()
    JSON.print(buf,Dict("metadata"=>d,"zarr_consolidated_format"=>1),4)
    s[p,".zmetadata"] = take!(buf)
  end
  ConsolidatedStore(s,p,d)
end

consolidate_metadata(s) = consolidate_metadata(zopen(s,"w"))
