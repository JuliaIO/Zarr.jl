# The public-but-not-exported names of `ZarrCore`: the developer-facing API,
# i.e. everything a downstream package or a new backend could need. The
# user-facing subset that `Zarr` marks public lives in `src/public_names_zarr.jl`.
#
# Only bare `public` statements belong in this file: `Zarr` parses it verbatim
# on Julia 1.10, where `public` is not a keyword and `names()` cannot report
# public names. See `Zarr._declared_public_names`.

public zname, zopen_noerr

# Stores. Every store type is public; `DirectoryStore` is additionally exported
# for backwards compatibility. Stores that live in a subpackage declare
# themselves over there: `HTTPStore` in ZarrHTTP, `ZipStore` in ZarrZip, and
# `GCStore`/`S3Store` -- which are exported, not merely public -- in
# ZarrGCS/ZarrS3.
public AbstractStore, DictStore, CachingStore, ConsolidatedStore

public consolidate_metadata

# The interface a new store backend has to implement, see `?AbstractStore`.
# (`storagesize` is part of it too, but is exported above.)
public subdirs, subkeys, isinitialized, storefromstring,
    store_read_strategy, SequentialRead, ConcurrentRead, storageregexlist,
    cloud_list_objects, concurrent_io_tasks, missing_chunk_return_code!

# Chunk key encodings and the registry used to add new ones.
public AbstractChunkKeyEncoding, ChunkKeyEncoding, SuffixChunkKeyEncoding,
    citostring, register_chunk_key_encoding, parse_chunk_key_encoding,
    lower_chunk_key_encoding

# Filters and the interface a new filter has to implement, see `?Filter`.
public Filter, VLenArrayFilter, VLenUTF8Filter, Fletcher32Filter,
    FixedScaleOffsetFilter, ShuffleFilter, QuantizeFilter, DeltaFilter
public zencode, zdecode, getfilter, sourcetype, desttype, filterdict,
    register_filter

# Compressors and the interface a new compressor has to implement. The concrete
# compressors live in subpackages (`BloscCompressor` in ZarrBlosc,
# `ZlibCompressor` in ZarrZlib, `ZstdCompressor` in ZarrZstd) and declare
# themselves public over there.
public Compressor, NoCompressor
public zcompress, zcompress!, zuncompress, zuncompress!, getCompressor,
    compressortypes, DEFAULT_COMPRESSOR, v2_to_v3_codecs

# v3 codecs and the interface a new codec has to implement, see `?Codec`.
# Codecs that live in a subpackage (`BloscV3Codec` in ZarrBlosc, `GzipV3Codec`
# in ZarrZlib, `ZstdV3Codec` in ZarrZstd) declare themselves public over there.
public Codecs, Codec, V3Codec, BytesCodec, CRC32cCodec, ShardingCodec,
    TransposeCodec, CRC32cV3Codec, VLenUTF8V3Codec

# Data type and fill value encoding, needed to map Zarr dtypes to Julia types.
public typestr, fill_value_encoding, fill_value_decoding
