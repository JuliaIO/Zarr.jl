module Zarr

using ZarrCore
import JSON
import Blosc

# Import names that will be extended by included files (import allows method extension)
import ZarrCore: getCompressor, zuncompress, zuncompress!, zcompress, zcompress!,
    storagesize, storefromstring, store_read_strategy,
    subdirs, subkeys, isinitialized,
    consolidate_metadata, compressor_to_v3_bytes_codecs

# Import types, non-extended functions, and constants via using
using ZarrCore: ZarrCore,
    # Types
    ZArray, ZGroup, AbstractStore, DirectoryStore, DictStore, ConsolidatedStore,
    AbstractMetadata, MetadataV2, MetadataV3,
    AbstractCodecPipeline, V2Pipeline, V3Pipeline,
    Compressor, NoCompressor,
    Filter,
    ZarrFormat,
    AbstractChunkKeyEncoding, ChunkKeyEncoding, SuffixChunkKeyEncoding,
    ASCIIChar,
    # Functions (not extended)
    zcreate, zopen, zzeros, zgroup,
    storageratio, zinfo, zname,
    pipeline_encode, pipeline_decode!, get_pipeline, get_order,
    Metadata, Metadata3,
    typestr, typestr3,
    fill_value_encoding, fill_value_decoding,
    compressortypes, default_compressor,
    getfilters, filterdict,
    zencode, zdecode,
    citostring,
    default_sep, default_prefix,
    storageregexlist,
    chunkindices,
    _reinterpret,
    # Storage functions (not extended)
    SequentialRead, ConcurrentRead,
    concurrent_io_tasks,
    is_zarray, is_zgroup, is_zarr2, is_zarr3,
    getmetadata, writemetadata, getattrs, writeattrs,
    isemptysub, _concatpath,
    store_readchunk, store_writechunk, store_deletechunk, store_isinitialized,
    read_items!, write_items!, channelsize, maybecopy,
    # Storage utility functions
    normalize_path,
    # Filter types
    VLenArrayFilter, VLenUTF8Filter,
    Fletcher32Filter, FixedScaleOffsetFilter, ShuffleFilter,
    QuantizeFilter, DeltaFilter,
    # DateTime support
    DateTime64,
    # Constants
    DV, DS, DS2, DS3

# Re-export the same symbols as before
export ZArray, ZGroup, zopen, zzeros, zcreate, storagesize, storageratio,
  zinfo, DirectoryStore, DictStore, ConsolidatedStore, zgroup

# Bring in the MaxLengthStrings submodule
using ZarrCore: MaxLengthStrings
using .MaxLengthStrings: MaxLengthString

# Include compressor implementations (they register into ZarrCore.compressortypes)
include("Compressors/blosc.jl")
include("Compressors/zlib.jl")
include("Compressors/zstd.jl")

# Override default compressor and register compressors/codecs/stores at module init time
# (Dict/Array mutations during precompile don't persist, so they must happen in __init__)
function __init__()
    ZarrCore.DEFAULT_COMPRESSOR_FACTORY[] = () -> BloscCompressor()

    # Register compressor types
    compressortypes["blosc"] = BloscCompressor
    compressortypes["zlib"] = ZlibCompressor
    compressortypes["zstd"] = ZstdCompressor

    # Register V3 codec parsers
    _register_v3_codec_parsers!()

    # Register storage URL resolvers
    _register_storage_regexes!()
end

function _register_storage_regexes!()
    # GCStore regexes (must be first to match before generic HTTP)
    pushfirst!(storageregexlist, r"^https://storage.googleapis.com" => GCStore)
    pushfirst!(storageregexlist, r"^http://storage.googleapis.com" => GCStore)
    push!(storageregexlist, r"^gs://" => GCStore)
    # HTTPStore regexes
    push!(storageregexlist, r"^https://" => HTTPStore)
    push!(storageregexlist, r"^http://" => HTTPStore)
    # S3Store regex
    push!(storageregexlist, r"^s3://" => S3Store)
end

# Include V3 compression codec implementations
include("Codecs/V3/compression_codecs.jl")

# Now define the compressor-to-v3 codec mappings
function compressor_to_v3_bytes_codecs(c::BloscCompressor)
    (BloscV3Codec(c.cname, c.clevel, c.shuffle, c.blocksize, sizeof(UInt8)),)
end
function compressor_to_v3_bytes_codecs(c::ZlibCompressor)
    level = c.config.level == -1 ? 6 : c.config.level
    (GzipV3Codec(level),)
end
function compressor_to_v3_bytes_codecs(c::ZstdCompressor)
    (ZstdV3Codec(c.config.compressionLevel),)
end

# Create a Codecs module wrapper so that Zarr.Codecs.V3Codecs.XXX paths work for tests.
# This re-exports everything from ZarrCore.Codecs.V3Codecs plus the compression codecs
# defined in Zarr.
module Codecs
    module V3Codecs
        # Re-export everything from ZarrCore.Codecs.V3Codecs
        using ZarrCore.Codecs.V3Codecs: V3Codec, v3_codec_parsers, codec_to_dict,
            codec_encode, codec_decode, codec_category, encoded_shape,
            BytesCodec, TransposeCodec
        # Re-export the compression codecs defined in Zarr
        using ...Zarr: BloscV3Codec, GzipV3Codec, ZstdV3Codec, CRC32cV3Codec, ShardingCodec
    end
end

# Include network/archive storage backends
include("Storage/gcstore.jl")
include("Storage/http.jl")
include("Storage/zipstore.jl")

# Register S3Store stub
struct S3Store <: AbstractStore
    bucket::String
    aws::Any
end
function S3Store(args...)
    error("AWSS3 must be loaded to use S3Store. Try `using AWSS3`.")
end

# Store URL resolvers registered in __init__

# HTTP.serve and writezip for ZArray/ZGroup
HTTP.serve(s::Union{ZArray,ZGroup}, args...; kwargs...) = HTTP.serve(s.storage, s.path, args...; kwargs...)
writezip(io::IO, s::Union{ZArray,ZGroup}; kwargs...) = writezip(io, s.storage, s.path; kwargs...)

export S3Store, GCStore

end # module
