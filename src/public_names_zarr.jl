# `Zarr`'s public-but-not-exported API: the user-facing subset of what the
# subpackages declare. The extension points a *new backend* needs (`subdirs`,
# `storefromstring`, `zencode`, `typestr`, ...) stay public where they are
# defined -- `ZarrCore.public_names_core.jl` and friends -- rather than being
# advertised a second time here. Those bindings still exist on `Zarr`; they are
# simply not part of its own public surface.
#
# Only bare `public` statements belong in this file: it is parsed verbatim by
# `Zarr._declared_public_names`.

public zname

# Stores. Every store type is public; `DirectoryStore`, `S3Store` and `GCStore`
# are additionally exported for backwards compatibility. `HTTPStore` comes from
# ZarrHTTP and `ZipStore`/`writezip` from ZarrZip.
public DictStore, HTTPStore, ZipStore, CachingStore, ConsolidatedStore

public consolidate_metadata, writezip, missing_chunk_return_code!,
    gcs_credentials

# Chunk key encodings and the registry used to add new ones.
public ChunkKeyEncoding, SuffixChunkKeyEncoding

# Filters and the interface a new filter has to implement, see `?Filter`.
public Filter, VLenArrayFilter, VLenUTF8Filter, Fletcher32Filter,
    FixedScaleOffsetFilter, ShuffleFilter, QuantizeFilter, DeltaFilter

# Compressors. The concrete ones live in ZarrBlosc/ZarrZlib/ZarrZstd.
public Compressor, NoCompressor, BloscCompressor, ZlibCompressor, ZstdCompressor

# v3 codecs and the interface a new codec has to implement, see `?Codec`.
# `BloscV3Codec`/`GzipV3Codec`/`ZstdV3Codec` live in the compressor subpackages.
public Codecs, Codec, V3Codec, BytesCodec, CRC32cCodec, ShardingCodec,
    TransposeCodec, GzipV3Codec, BloscV3Codec, ZstdV3Codec, CRC32cV3Codec,
    VLenUTF8V3Codec
