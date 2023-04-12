module Zarr

import JSON
import Blosc

include("metadata.jl")
include("Compressors.jl")
include("Storage/Storage.jl")
include("Filters.jl")
include("ZArray.jl")
include("ZGroup.jl")

export ZArray, ZGroup, zopen, zzeros, zcreate, storagesize, storageratio,
  zinfo, DirectoryStore, S3Store, GCStore, zgroup

end # module
