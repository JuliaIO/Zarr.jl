# Extension API. `Zarr` parses this file on Julia 1.10, so keep only `public`
# statements and comments.

public zname, zopen_noerr

# Core stores; backend stores are declared by their packages.
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

# Compressor interface; concrete compressors are declared by their packages.
public Compressor, NoCompressor
public zcompress, zcompress!, zuncompress, zuncompress!, getCompressor,
    compressortypes, DEFAULT_COMPRESSOR, v2_to_v3_codecs

# Core v3 codecs; backend codecs are declared by their packages.
public Codecs, Codec, V3Codec, BytesCodec, CRC32cCodec, ShardingCodec,
    TransposeCodec, CRC32cV3Codec, VLenUTF8V3Codec

# Data type and fill value encoding, needed to map Zarr dtypes to Julia types.
public typestr, fill_value_encoding, fill_value_decoding
