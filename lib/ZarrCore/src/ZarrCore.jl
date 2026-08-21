module ZarrCore

import JSON
import Unicode
using OrderedCollections: OrderedDict

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
include("ZArray.jl")
include("pipeline.jl")
include("ZGroup.jl")
include("caching.jl")

import .Codecs: Codec
import .Codecs.V3Codecs: V3Codec, BytesCodec, CRC32cCodec,
    ShardingCodec, TransposeCodec, CRC32cV3Codec, VLenUTF8V3Codec

# User-facing exports. Extension APIs are declared in public_names_core.jl.

export ZArray, ZGroup, zopen, zzeros, zcreate, zgroup, zarrcache,
  storagesize, storageratio, zinfo,
  DirectoryStore

@static if VERSION >= v"1.11"
    include("public_names_core.jl")
end
end # module
