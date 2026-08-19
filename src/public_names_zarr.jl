public zname

# Stores. Every store type is public; `DirectoryStore`, `S3Store` and `GCStore`
# are additionally exported for backwards compatibility.
public DictStore, HTTPStore, ZipStore, CachingStore,
    ConsolidatedStore
    
public consolidate_metadata, writezip

# Chunk key encodings and the registry used to add new ones.
public ChunkKeyEncoding, SuffixChunkKeyEncoding

# Filters and the interface a new filter has to implement, see `?Filter`.
public Filter, VLenArrayFilter, VLenUTF8Filter, Fletcher32Filter,
    FixedScaleOffsetFilter, ShuffleFilter, QuantizeFilter, DeltaFilter

# Compressors and the interface a new compressor has to implement.
public Compressor, NoCompressor, BloscCompressor, ZlibCompressor, ZstdCompressor

# v3 codecs and the interface a new codec has to implement, see `?Codec`.
public Codecs, Codec, V3Codec, BloscCodec, BytesCodec, CRC32cCodec, GzipCodec,
    ShardingCodec, TransposeCodec, GzipV3Codec, BloscV3Codec, ZstdV3Codec,
    CRC32cV3Codec, VLenUTF8V3Codec

# Data type and fill value encoding, needed to map Zarr dtypes to Julia types.
public typestr, fill_value_encoding, fill_value_decoding