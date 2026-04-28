module Zarr

import JSON
import Blosc

struct ZarrFormat{V}
  version::Val{V}
end
Base.Int(v::ZarrFormat{V}) where V = V
@inline ZarrFormat(v::Int) = ZarrFormat(Val(v))
ZarrFormat(v::ZarrFormat) = v
#Default Zarr Version
const DV = ZarrFormat(Val(2))

include("types.jl")
include("chunkkeyencoding.jl")
include("metadata.jl")
include("metadata3.jl")
include("Compressors/Compressors.jl")
include("Codecs/Codecs.jl")
include("Storage/Storage.jl")
include("Filters/Filters.jl")
"""
    enable_partial_shard_storage_reads[]

When `true` (default), the sharded-chunk fast path in
[`readblock!`](@ref) issues byte-range reads to the storage backend
instead of loading the whole shard file. Stores opt in via
[`supports_partial_reads`](@ref); stores that don't are unaffected.

Set to `false` to fall back to the in-memory partial-decode path
(useful for A/B comparisons or to debug a suspected partial-read bug).
"""
const enable_partial_shard_storage_reads = Ref(true)

include("ZArray.jl")
include("pipeline.jl")
include("ZGroup.jl")

export ZArray, ZGroup, zopen, zzeros, zcreate, storagesize, storageratio,
  zinfo, DirectoryStore, S3Store, GCStore, zgroup

end # module
