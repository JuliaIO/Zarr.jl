
struct ChunkEncoding
    sep::Char
    prefix::Bool
end

# Default Zarr v2 separator
const DS2 = '.'
# Default Zarr v3 separator
const DS3 = '/'

default_sep(::ZarrFormat{2}) = DS2
default_sep(::ZarrFormat{3}) = DS3
default_prefix(::ZarrFormat{2}) = false
default_prefix(::ZarrFormat{3}) = true
const DS = default_sep(DV)

@inline function citostring(e::ChunkEncoding, i::CartesianIndex)
  if e.prefix
    "c$(e.sep)" * join(reverse((i - oneunit(i)).I), e.sep)
  else
    join(reverse((i - oneunit(i)).I), e.sep)
  end
end
@inline citostring(e::ChunkEncoding, ::CartesianIndex{0}) = e.prefix ? "c$(e.sep)0" : "0"

_concatpath(p,s) = isempty(p) ? s : rstrip(p,'/') * '/' * s
