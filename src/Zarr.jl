module Zarr

import JSON
import Blosc

struct V2 end
struct V3 end
const ZARR_VERSION = Union{V2,V3}

include("metadata.jl")
include("metadatav3.jl")
include("Compressors.jl")
include("Storage/Storage.jl")
include("Filters.jl")
include("ZArray.jl")
include("ZGroup.jl")

export ZArray, ZGroup, zopen, zzeros, zcreate, storagesize, storageratio,
  zinfo, DirectoryStore, S3Store, GCStore, zgroup

end # module
