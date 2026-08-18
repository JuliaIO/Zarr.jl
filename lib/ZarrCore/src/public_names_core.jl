public zname, zopen_noerr

# Stores. Every store type is public; `DirectoryStore`, `S3Store` and `GCStore`
# are additionally exported for backwards compatibility.
public AbstractStore, DictStore, HTTPStore, ZipStore, CachingStore,
    ConsolidatedStore
    
public consolidate_metadata, writezip

# The interface a new store backend has to implement, see `?AbstractStore`.
# (`storagesize` is part of it too, but is exported above.)
public subdirs, subkeys, isinitialized, storefromstring,
    store_read_strategy, SequentialRead, ConcurrentRead, storageregexlist,
    cloud_list_objects, concurrent_io_tasks

# Chunk key encodings and the registry used to add new ones.
public AbstractChunkKeyEncoding, ChunkKeyEncoding, SuffixChunkKeyEncoding,
    citostring, register_chunk_key_encoding, parse_chunk_key_encoding,
    lower_chunk_key_encoding

# Filters and the interface a new filter has to implement, see `?Filter`.
public Filter, VLenArrayFilter, VLenUTF8Filter, Fletcher32Filter,
    FixedScaleOffsetFilter, ShuffleFilter, QuantizeFilter, DeltaFilter
public zencode, zdecode, getfilter, sourcetype, desttype, filterdict

# Compressors and the interface a new compressor has to implement.
public Compressor, NoCompressor, BloscCompressor, ZlibCompressor, ZstdCompressor
public zcompress, zcompress!, zuncompress, zuncompress!, getCompressor,
    compressortypes

# v3 codecs and the interface a new codec has to implement, see `?Codec`.
public Codecs, Codec, V3Codec, BloscCodec, BytesCodec, CRC32cCodec, GzipCodec,
    ShardingCodec, TransposeCodec, GzipV3Codec, BloscV3Codec, ZstdV3Codec,
    CRC32cV3Codec, VLenUTF8V3Codec

# Data type and fill value encoding, needed to map Zarr dtypes to Julia types.
# `DateTime64` is re-exported from DateTimes64.jl because it shows up as the
# `eltype` of datetime arrays.
public typestr, fill_value_encoding, fill_value_decoding