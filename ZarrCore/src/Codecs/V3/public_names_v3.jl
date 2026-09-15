# The codec types themselves are re-exported (as public names) from `ZarrCore`;
# these are the extension points for defining and registering new v3 codecs.
public V3Codec, getCodec, register_codec, codec_parsers, codec_encode,
    codec_decode, is_fixed_size, name
public BytesCodec, CRC32cCodec, ShardingCodec, TransposeCodec, CRC32cV3Codec,
    VLenUTF8V3Codec

# Statically typed (juliac `--trim=safe`) codec construction: the extension
# points a codec package implements for the typed v3 open path.
public codec_name, pipeline_from_json
