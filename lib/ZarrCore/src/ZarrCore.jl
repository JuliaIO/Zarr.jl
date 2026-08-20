module ZarrCore

import JSON
import Blosc
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
import .Codecs.V3Codecs: V3Codec, BloscCodec, BytesCodec, CRC32cCodec, GzipCodec,
    ShardingCodec, TransposeCodec, GzipV3Codec, BloscV3Codec, ZstdV3Codec,
    CRC32cV3Codec, VLenUTF8V3Codec

# ## Public API
#
# The rule of thumb is: anything a downstream consumer could need, and every
# documented extension point, is part of the public API. `export` is reserved
# for the handful of names that are convenient to have in scope after
# `using Zarr`; everything else is marked `public` and must be qualified.
#
# Things that are deliberately *not* public (and may change without notice):
# `Metadata`/`MetadataV2`/`MetadataV3`, `ZarrFormat`, `is_zarray`, `is_zgroup`,
# `normalize_path`, `MaxLengthString`, `ShapeOnlyArray` and the various
# `pipeline_*`/`store_*` internals.

export ZArray, ZGroup, zopen, zzeros, zcreate, zgroup, zarrcache,
  storagesize, storageratio, zinfo,
  DirectoryStore, S3Store, GCStore

@static if VERSION >= v"1.11"
    include("public_names_core.jl")
  end
end # module
