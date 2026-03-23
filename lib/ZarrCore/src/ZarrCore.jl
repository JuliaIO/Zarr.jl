module ZarrCore

import JSON

struct ZarrFormat{V}
  version::Val{V}
end
Base.Int(v::ZarrFormat{V}) where V = V
@inline ZarrFormat(v::Int) = ZarrFormat(Val(v))
ZarrFormat(v::ZarrFormat) = v
const DV = ZarrFormat(Val(2))

abstract type AbstractCodecPipeline end

include("chunkkeyencoding.jl")
include("Compressors/Compressors.jl")
include("Codecs/Codecs.jl")
include("Filters/Filters.jl")
include("metadata.jl")
include("metadata3.jl")
include("pipeline.jl")
include("Storage/Storage.jl")
include("ZArray.jl")
include("ZGroup.jl")

export ZArray, ZGroup, zopen, zzeros, zcreate, storagesize, storageratio,
  zinfo, DirectoryStore, DictStore, ConsolidatedStore, zgroup

end # module
