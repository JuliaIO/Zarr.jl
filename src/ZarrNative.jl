module ZarrNative

include("Storage.jl")
include("Compressors.jl")
include("ZArray.jl")
include("ZGroup.jl")

using .ZArrays, .ZGroups
export ZArray, ZGroup, zopen, zzeros

end # module
