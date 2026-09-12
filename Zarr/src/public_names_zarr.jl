# User-facing public API. Extension APIs remain public in their defining package.
# Parsed by `Zarr` on Julia 1.10; keep only `public` statements and comments.

public zname

# Stores
public DictStore, HTTPStore, ZipStore, CachingStore, ConsolidatedStore

public consolidate_metadata, writezip, missing_chunk_return_code!,
    gcs_credentials

# Chunk key encodings and the registry used to add new ones.
public ChunkKeyEncoding, SuffixChunkKeyEncoding

# Filters and the interface a new filter has to implement, see `?Filter`.
public Filter, VLenArrayFilter, VLenUTF8Filter, Fletcher32Filter,
    FixedScaleOffsetFilter, ShuffleFilter, QuantizeFilter, DeltaFilter

# Compressors
public Compressor, NoCompressor, BloscCompressor, ZlibCompressor, ZstdCompressor

# v3 codecs
public Codecs, Codec, V3Codec, BytesCodec, CRC32cCodec, ShardingCodec,
    TransposeCodec, GzipV3Codec, BloscV3Codec, ZstdV3Codec, CRC32cV3Codec,
    VLenUTF8V3Codec

# Data type and fill value encoding
public typestr, fill_value_encoding, fill_value_decoding
