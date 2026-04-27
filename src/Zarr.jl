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

"""
    enable_threaded_shard_decode[]

When `true` (default) and Julia is running with more than one thread,
inner-chunk decompresses in `read_shard_partial_with_source!` are
dispatched to `Threads.@spawn` so that one shard read scales with
available cores. Falls back to a sequential loop on single-threaded
runs or when the work list has fewer than two entries.

Set to `false` to force the sequential path even with `-t > 1`
(useful for debugging or for callers that already parallelize at a
higher level).
"""
const enable_threaded_shard_decode = Ref(true)

include("types.jl")
include("chunkkeyencoding.jl")
include("metadata.jl")
include("metadata3.jl")
include("Compressors/Compressors.jl")
include("Codecs/Codecs.jl")
include("Storage/Storage.jl")
include("Filters/Filters.jl")
include("ZArray.jl")
include("pipeline.jl")
include("ZGroup.jl")

export ZArray, ZGroup, zopen, zzeros, zcreate, storagesize, storageratio,
  zinfo, DirectoryStore, S3Store, GCStore, zgroup

end # module
