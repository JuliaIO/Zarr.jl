module ZarrCore

import JSON
import Unicode
using OrderedCollections: OrderedDict

"""
    PUBLIC_NAMES::Vector{Symbol}

Every name this module declares with [`@public`](@ref), in declaration order.

On Julia 1.11+ this duplicates what `names(ZarrCore)` already reports, but on
1.10 the `public` keyword does not exist, so this registry is the only record
of the public-but-not-exported API. The `Zarr` facade uses it to re-export the
same set of names on every supported Julia version.

Like `public` itself, this is per-module: every module that uses `@public` owns
a `PUBLIC_NAMES`, and a submodule's public names live in *its* list, not here.
"""
const PUBLIC_NAMES = Symbol[]

"""
    @public name
    @public name1, name2, ...

Mark `name`s as public API without exporting them, i.e. `Base.ispublic` returns
`true` and they show up in `names(ZarrCore)`, but `using ZarrCore` does not
bring them into scope.

This is the `public` keyword, which only exists from Julia 1.11 onwards. On
Julia 1.10 the declaration itself expands to nothing, so the names are recorded
in the calling module's [`PUBLIC_NAMES`](@ref) as well -- `names()` cannot
report them there. A module using `@public` must therefore define its own
`const PUBLIC_NAMES = Symbol[]`.
"""
macro public(ex)
    syms = ex isa Symbol ? [ex] : ex.args
    all(s -> s isa Symbol, syms) ||
        throw(ArgumentError("@public expects one or more symbols, got $ex"))
    decl = VERSION >= v"1.11" ? esc(Expr(:public, syms...)) : nothing
    quote
        append!($(esc(:PUBLIC_NAMES)), $(QuoteNode(syms)))
        $decl
    end
end

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

# ## Public API
#
# The rule of thumb is: anything a downstream consumer could need, and every
# documented extension point, is part of the public API. `export` is reserved
# for the handful of names that are convenient to have in scope after
# `using Zarr`; everything else is marked `@public` and must be qualified.
#
# Things that are deliberately *not* public (and may change without notice):
# `Metadata`/`MetadataV2`/`MetadataV3`, `ZarrFormat`, `is_zarray`, `is_zgroup`,
# `normalize_path`, `MaxLengthString`, `ShapeOnlyArray` and the various
# `pipeline_*`/`store_*` internals.

export ZArray, ZGroup, zopen, zzeros, zcreate, zgroup, zarrcache,
  storagesize, storageratio, zinfo,
  DirectoryStore

@public zname, zopen_noerr

# Stores. Every store type is public; `DirectoryStore` is additionally exported
# for backwards compatibility. Stores that live in a subpackage (`ZipStore` in
# ZarrZip, `HTTPStore` in ZarrHTTP, `GCStore` in ZarrGCS, `S3Store` in ZarrS3)
# declare themselves public -- or, for `GCStore` and `S3Store`, exported --
# over there.
@public AbstractStore, DictStore, CachingStore,
    ConsolidatedStore, PermanentZarrCache
@public consolidate_metadata

# The interface a new store backend has to implement, see `?AbstractStore`.
# (`storagesize` is part of it too, but is exported above.)
@public subdirs, subkeys, isinitialized, storefromstring,
    store_read_strategy, SequentialRead, ConcurrentRead, storageregexlist,
    cloud_list_objects, concurrent_io_tasks, missing_chunk_return_code!

# Chunk key encodings and the registry used to add new ones.
@public AbstractChunkKeyEncoding, ChunkKeyEncoding, SuffixChunkKeyEncoding,
    citostring, register_chunk_key_encoding, parse_chunk_key_encoding,
    lower_chunk_key_encoding

# Filters and the interface a new filter has to implement, see `?Filter`.
@public Filter, VLenArrayFilter, VLenUTF8Filter, Fletcher32Filter,
    FixedScaleOffsetFilter, ShuffleFilter, QuantizeFilter, DeltaFilter
@public zencode, zdecode, getfilter, sourcetype, desttype, filterdict, register_filter

# Compressors and the interface a new compressor has to implement. The concrete
# compressors live in subpackages (`BloscCompressor` in ZarrBlosc,
# `ZlibCompressor` in ZarrZlib, `ZstdCompressor` in ZarrZstd) and declare
# themselves public over there.
@public Compressor, NoCompressor
@public zcompress, zcompress!, zuncompress, zuncompress!, getCompressor,
    compressortypes, DEFAULT_COMPRESSOR, v2_to_v3_codecs

# v3 codecs and the interface a new codec has to implement, see `?Codec`.
# Codecs that live in a subpackage (`BloscV3Codec`, `GzipV3Codec`,
# `ZstdV3Codec`) declare themselves public over there.
@public Codecs, Codec, V3Codec, BytesCodec, CRC32cCodec,
    ShardingCodec, TransposeCodec, CRC32cV3Codec, VLenUTF8V3Codec

# Data type and fill value encoding, needed to map Zarr dtypes to Julia types.
# `DateTime64` is re-exported from DateTimes64.jl because it shows up as the
# `eltype` of datetime arrays.
@public typestr, fill_value_encoding, fill_value_decoding, DateTime64

end # module
