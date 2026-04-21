using Test
using Zarr
using JSON

@testset "V3 Codecs" begin

@testset "BytesCodec" begin
    data = Int32[1, 2, 3, 4]

    # little-endian round-trip
    codec_le = Zarr.Codecs.V3Codecs.BytesCodec(:little)
    encoded_le = Zarr.Codecs.V3Codecs.codec_encode(codec_le, data)
    @test encoded_le isa Vector{UInt8}
    @test length(encoded_le) == 16
    decoded_le = Zarr.Codecs.V3Codecs.codec_decode(codec_le, encoded_le, Int32, (4,))
    @test decoded_le == data

    # big-endian round-trip
    codec_be = Zarr.Codecs.V3Codecs.BytesCodec(:big)
    encoded_be = Zarr.Codecs.V3Codecs.codec_encode(codec_be, data)
    @test encoded_be isa Vector{UInt8}
    @test length(encoded_be) == 16
    decoded_be = Zarr.Codecs.V3Codecs.codec_decode(codec_be, encoded_be, Int32, (4,))
    @test decoded_be == data

    # little and big-endian produce different bytes for multi-byte types
    @test encoded_le != encoded_be

    # cross-decoding: big-endian bytes decoded with big-endian codec == original
    decoded_cross = Zarr.Codecs.V3Codecs.codec_decode(codec_le, encoded_be, Int32, (4,))
    @test decoded_cross != data  # mismatched endian gives wrong values

    # default constructor uses little endian
    @test Zarr.Codecs.V3Codecs.BytesCodec().endian == :little

    # floating-point round-trips for both endian modes
    for (FT, data_f) in [
        (Float16, Float16[-1.5, 0.0, 1.5, 2.5]),
        (Float32, Float32[-1000.5, 0.0, 1.0, 1000.5]),
        (Float64, Float64[-1.5e300, 0.0, 1.0, 1.5e300]),
    ]
        for codec in (codec_le, codec_be)
            encoded = Zarr.Codecs.V3Codecs.codec_encode(codec, data_f)
            @test encoded isa Vector{UInt8}
            @test length(encoded) == length(data_f) * sizeof(FT)
            decoded = Zarr.Codecs.V3Codecs.codec_decode(codec, encoded, FT, (4,))
            @test decoded == data_f
        end
        # little and big-endian encodings differ for multi-byte floats
        enc_le = Zarr.Codecs.V3Codecs.codec_encode(codec_le, data_f)
        enc_be = Zarr.Codecs.V3Codecs.codec_encode(codec_be, data_f)
        @test enc_le != enc_be
    end
end

@testset "TransposeCodec" begin
    codec_c = Zarr.Codecs.V3Codecs.TransposeCodec((1, 2, 3))
    data = reshape(collect(1:24), 2, 3, 4)
    encoded = Zarr.Codecs.V3Codecs.codec_encode(codec_c, data)
    @test encoded == data

    codec_f = Zarr.Codecs.V3Codecs.TransposeCodec((3, 2, 1))
    encoded_f = Zarr.Codecs.V3Codecs.codec_encode(codec_f, data)
    decoded_f = Zarr.Codecs.V3Codecs.codec_decode(codec_f, encoded_f)
    @test decoded_f == data
end

@testset "get_order" begin
    bytes_codec = Zarr.Codecs.V3Codecs.BytesCodec()

    # No array->array codecs → 'C'
    p = Zarr.V3Pipeline((), bytes_codec, ())
    md = Zarr.MetadataV3{Int32,3,typeof(p)}(3, "array", (3,3,3), (3,3,3), "int32", p, Int32(0), Zarr.ChunkKeyEncoding('/',true))
    @test Zarr.get_order(md) == 'C'

    # Single TransposeCodec with identity permutation → 'C'
    tc_c = Zarr.Codecs.V3Codecs.TransposeCodec((1,2,3))
    p = Zarr.V3Pipeline((tc_c,), bytes_codec, ())
    md = Zarr.MetadataV3{Int32,3,typeof(p)}(3, "array", (3,3,3), (3,3,3), "int32", p, Int32(0), Zarr.ChunkKeyEncoding('/',true))
    @test Zarr.get_order(md) == 'C'

    # Single TransposeCodec with reverse permutation → 'F'
    tc_f = Zarr.Codecs.V3Codecs.TransposeCodec((3,2,1))
    p = Zarr.V3Pipeline((tc_f,), bytes_codec, ())
    md = Zarr.MetadataV3{Int32,3,typeof(p)}(3, "array", (3,3,3), (3,3,3), "int32", p, Int32(0), Zarr.ChunkKeyEncoding('/',true))
    @test Zarr.get_order(md) == 'F'

    # Single TransposeCodec with arbitrary (non-C, non-F) permutation → ArgumentError
    tc_other = Zarr.Codecs.V3Codecs.TransposeCodec((2,1,3))
    p = Zarr.V3Pipeline((tc_other,), bytes_codec, ())
    md = Zarr.MetadataV3{Int32,3,typeof(p)}(3, "array", (3,3,3), (3,3,3), "int32", p, Int32(0), Zarr.ChunkKeyEncoding('/',true))
    @test_throws ArgumentError Zarr.get_order(md)

    # Multiple array->array codecs → ArgumentError
    p = Zarr.V3Pipeline((tc_f, tc_f), bytes_codec, ())
    md = Zarr.MetadataV3{Int32,3,typeof(p)}(3, "array", (3,3,3), (3,3,3), "int32", p, Int32(0), Zarr.ChunkKeyEncoding('/',true))
    @test_throws ArgumentError Zarr.get_order(md)

    # Unrecognized array->array codec type → ArgumentError
    struct _FakeCodec <: Zarr.Codecs.V3Codecs.V3Codec{:array,:array} end
    p = Zarr.V3Pipeline((_FakeCodec(),), bytes_codec, ())
    md = Zarr.MetadataV3{Int32,3,typeof(p)}(3, "array", (3,3,3), (3,3,3), "int32", p, Int32(0), Zarr.ChunkKeyEncoding('/',true))
    @test_throws ArgumentError Zarr.get_order(md)
end

@testset "GzipV3Codec" begin
    codec = Zarr.Codecs.V3Codecs.GzipV3Codec(6)
    data = reinterpret(UInt8, Int32[1, 2, 3, 4]) |> collect
    encoded = Zarr.Codecs.V3Codecs.codec_encode(codec, data)
    @test encoded isa Vector{UInt8}
    decoded = Zarr.Codecs.V3Codecs.codec_decode(codec, encoded)
    @test decoded == data
end

@testset "BloscV3Codec" begin
    codec = Zarr.Codecs.V3Codecs.BloscV3Codec("lz4", 5, 0, 0, 4)
    data = reinterpret(UInt8, Int32[1, 2, 3, 4]) |> collect
    encoded = Zarr.Codecs.V3Codecs.codec_encode(codec, data)
    @test encoded isa Vector{UInt8}
    decoded = Zarr.Codecs.V3Codecs.codec_decode(codec, encoded)
    @test decoded == data
end

@testset "BloscV3Codec shuffle parameter" begin
    data = reinterpret(UInt8, Int32[1, 2, 3, 4]) |> collect

    # --- encode/decode round-trip for each shuffle mode ---
    for (shuffle_str, shuffle_int) in (("noshuffle", 0), ("shuffle", 1), ("bitshuffle", 2))
        codec = Zarr.Codecs.V3Codecs.BloscV3Codec("lz4", 5, shuffle_int, 0, 4)
        encoded = Zarr.Codecs.V3Codecs.codec_encode(codec, data)
        @test encoded isa Vector{UInt8}
        decoded = Zarr.Codecs.V3Codecs.codec_decode(codec, encoded)
        @test decoded == data
    end

    # --- metadata parsing: shuffle string -> integer ---
    for (shuffle_str, expected_int) in (("noshuffle", 0), ("shuffle", 1), ("bitshuffle", 2))
        json_str = """{"zarr_format":3,"node_type":"array","shape":[4],"data_type":"int32",
            "chunk_grid":{"name":"regular","configuration":{"chunk_shape":[4]}},
            "chunk_key_encoding":{"name":"default","configuration":{"separator":"/"}},
            "fill_value":0,"codecs":[
                {"name":"bytes","configuration":{"endian":"little"}},
                {"name":"blosc","configuration":{"cname":"lz4","clevel":5,"shuffle":"$shuffle_str","blocksize":0,"typesize":4}}
            ]}"""
        md = Zarr.Metadata(json_str, false)
        pipeline = Zarr.get_pipeline(md)
        blosc = pipeline.bytes_bytes[1]
        @test blosc isa Zarr.Codecs.V3Codecs.BloscV3Codec
        @test blosc.shuffle == expected_int
    end

    # --- metadata parsing: integer shuffle passthrough ---
    for (shuffle_int, expected_int) in ((0, 0), (1, 1), (2, 2))
        json_str = """{"zarr_format":3,"node_type":"array","shape":[4],"data_type":"int32",
            "chunk_grid":{"name":"regular","configuration":{"chunk_shape":[4]}},
            "chunk_key_encoding":{"name":"default","configuration":{"separator":"/"}},
            "fill_value":0,"codecs":[
                {"name":"bytes","configuration":{"endian":"little"}},
                {"name":"blosc","configuration":{"cname":"lz4","clevel":5,"shuffle":$shuffle_int,"blocksize":0,"typesize":4}}
            ]}"""
        md = Zarr.Metadata(json_str, false)
        pipeline = Zarr.get_pipeline(md)
        blosc = pipeline.bytes_bytes[1]
        @test blosc.shuffle == expected_int
    end

    # --- metadata parsing: unknown shuffle string raises ArgumentError ---
    bad_json = """{"zarr_format":3,"node_type":"array","shape":[4],"data_type":"int32",
        "chunk_grid":{"name":"regular","configuration":{"chunk_shape":[4]}},
        "chunk_key_encoding":{"name":"default","configuration":{"separator":"/"}},
        "fill_value":0,"codecs":[
            {"name":"bytes","configuration":{"endian":"little"}},
            {"name":"blosc","configuration":{"cname":"lz4","clevel":5,"shuffle":"invalid","blocksize":0,"typesize":4}}
        ]}"""
    @test_throws ArgumentError Zarr.Metadata(bad_json, false)

    # --- serialization: integer -> shuffle string ---
    for (shuffle_int, expected_str) in ((0, "noshuffle"), (1, "shuffle"), (2, "bitshuffle"))
        json_str = """{"zarr_format":3,"node_type":"array","shape":[4],"data_type":"int32",
            "chunk_grid":{"name":"regular","configuration":{"chunk_shape":[4]}},
            "chunk_key_encoding":{"name":"default","configuration":{"separator":"/"}},
            "fill_value":0,"codecs":[
                {"name":"bytes","configuration":{"endian":"little"}},
                {"name":"blosc","configuration":{"cname":"lz4","clevel":5,"shuffle":$shuffle_int,"blocksize":0,"typesize":4}}
            ]}"""
        md = Zarr.Metadata(json_str, false)
        lowered = JSON.lower(md)
        blosc_config = lowered["codecs"][2]["configuration"]
        @test blosc_config["shuffle"] == expected_str
    end

    # --- serialization: unknown shuffle integer raises ArgumentError via lower3 ---
    let bad_blosc = Zarr.Codecs.V3Codecs.BloscV3Codec("lz4", 5, 99, 0, 4),
        bytes_codec = Zarr.Codecs.V3Codecs.BytesCodec(),
        bad_pipeline = Zarr.V3Pipeline((), bytes_codec, (bad_blosc,))
        bad_md = Zarr.MetadataV3{Int32,1,typeof(bad_pipeline)}(
            3, "array", (4,), (4,), "int32", bad_pipeline, Int32(0),
            Zarr.ChunkKeyEncoding('/', true)
        )
        @test_throws ArgumentError JSON.lower(bad_md)
    end
end

@testset "ZstdV3Codec" begin
    codec = Zarr.Codecs.V3Codecs.ZstdV3Codec(3)
    data = reinterpret(UInt8, Float64[1.5, 2.5, 3.5, 4.5]) |> collect
    encoded = Zarr.Codecs.V3Codecs.codec_encode(codec, data)
    @test encoded isa Vector{UInt8}
    decoded = Zarr.Codecs.V3Codecs.codec_decode(codec, encoded)
    @test decoded == data
end

@testset "CRC32cV3Codec" begin
    codec = Zarr.Codecs.V3Codecs.CRC32cV3Codec()
    data = UInt8[1, 2, 3, 4, 5, 6, 7, 8]
    encoded = Zarr.Codecs.V3Codecs.codec_encode(codec, data)
    @test length(encoded) == length(data) + 4
    decoded = Zarr.Codecs.V3Codecs.codec_decode(codec, encoded)
    @test decoded == data
end

@testset "BytesCodec endian validation" begin
    # Valid endian values are accepted
    @test Zarr.Codecs.V3Codecs.BytesCodec(:little).endian == :little
    @test Zarr.Codecs.V3Codecs.BytesCodec(:big).endian == :big

    # Invalid endian value throws ArgumentError
    @test_throws ArgumentError Zarr.Codecs.V3Codecs.BytesCodec(:invalid)
    @test_throws ArgumentError Zarr.Codecs.V3Codecs.BytesCodec(:native)
end

@testset "Non-canonical TransposeCodec in ZArray" begin
    # A permutation like (2,1,3) is not C or F order — get_order should throw,
    # but data reads and writes must still work correctly.
    tc = Zarr.Codecs.V3Codecs.TransposeCodec((2, 1, 3))
    bytes_codec = Zarr.Codecs.V3Codecs.BytesCodec()
    pipeline = Zarr.V3Pipeline((tc,), bytes_codec, ())
    md = Zarr.MetadataV3{Int32,3,typeof(pipeline)}(
        3, "array", (2,3,4), (2,3,4), "int32", pipeline, Int32(0),
        Zarr.ChunkKeyEncoding('/', true)
    )
    store = Zarr.DictStore()
    z = Zarr.ZArray(md, store, "", Dict(), true)
    data = reshape(Int32.(1:24), 2, 3, 4)
    z[:,:,:] = data
    @test z[:,:,:] == data

    # get_order throws for non-canonical permutation
    @test_throws ArgumentError Zarr.get_order(z.metadata)
end

@testset "V3 group attributes round-trip" begin
    store = Zarr.DictStore()
    g = zgroup(store, "", Zarr.ZarrFormat(3))
    zgroup(g, "sub"; attrs=Dict("key" => "val", "num" => 42))

    # Re-open the store and verify attributes are preserved
    g2 = zopen(store)
    @test g2["sub"].attrs["key"] == "val"
    @test g2["sub"].attrs["num"] == 42
end

@testset "CRC32c end-to-end ZArray" begin
    crc32c_codec = Zarr.Codecs.V3Codecs.CRC32cV3Codec()
    bytes_codec = Zarr.Codecs.V3Codecs.BytesCodec()
    pipeline = Zarr.V3Pipeline((), bytes_codec, (crc32c_codec,))
    md = Zarr.MetadataV3{Int32,1,typeof(pipeline),Zarr.ChunkKeyEncoding}(
        3, "array", (4,), (4,), "int32", pipeline, Int32(0),
        Zarr.ChunkKeyEncoding('/', true)
    )
    store = Zarr.DictStore()
    z = Zarr.ZArray(md, store, "", Dict(), true)
    data = Int32[10, 20, 30, 40]
    z[:] = data
    @test z[:] == data

    # CRC32c checksum corruption is detected on decode
    # Corrupt the stored bytes and verify an error is thrown
    key = only(keys(store.a))
    stored = copy(store.a[key])
    stored[end] = stored[end] ⊻ 0xFF   # flip bits in the checksum
    store.a[key] = stored
    @test_throws Exception z[:]
end

@testset "V2Pipeline encode/decode round-trip" begin
    comp = Zarr.BloscCompressor()
    pipeline = Zarr.V2Pipeline(comp, nothing)
    data = zeros(Int64, 4, 4)
    data[1, 1] = 42

    encoded = Zarr.pipeline_encode(pipeline, data, nothing)
    @test encoded isa Vector{UInt8}
    @test !isempty(encoded)

    output = zeros(Int64, 4, 4)
    Zarr.pipeline_decode!(pipeline, output, encoded)
    @test output == data
end

@testset "V2Pipeline with fill_value returns nothing" begin
    comp = Zarr.BloscCompressor()
    pipeline = Zarr.V2Pipeline(comp, nothing)
    data = fill(Int64(-1), 4, 4)
    encoded = Zarr.pipeline_encode(pipeline, data, Int64(-1))
    @test encoded === nothing
end

@testset "V3Pipeline encode/decode round-trip" begin
    bytes_codec = Zarr.Codecs.V3Codecs.BytesCodec()
    gzip_codec = Zarr.Codecs.V3Codecs.GzipV3Codec(6)
    pipeline = Zarr.V3Pipeline((), bytes_codec, (gzip_codec,))

    data = Int32[1, 2, 3, 4]
    encoded = Zarr.pipeline_encode(pipeline, data, nothing)
    @test encoded isa Vector{UInt8}

    output = zeros(Int32, 4)
    Zarr.pipeline_decode!(pipeline, output, encoded)
    @test output == data
end

@testset "V3Pipeline with no compression" begin
    bytes_codec = Zarr.Codecs.V3Codecs.BytesCodec()
    pipeline = Zarr.V3Pipeline((), bytes_codec, ())

    data = Float64[1.5, 2.5, 3.5]
    encoded = Zarr.pipeline_encode(pipeline, data, nothing)
    @test encoded isa Vector{UInt8}

    output = zeros(Float64, 3)
    Zarr.pipeline_decode!(pipeline, output, encoded)
    @test output == data
end

@testset "V3Pipeline fill_value returns nothing" begin
    bytes_codec = Zarr.Codecs.V3Codecs.BytesCodec()
    pipeline = Zarr.V3Pipeline((), bytes_codec, ())
    data = fill(Int32(0), 4)
    encoded = Zarr.pipeline_encode(pipeline, data, Int32(0))
    @test encoded === nothing
end

@testset "V3 Metadata Parsing" begin
    json_str = """{"zarr_format":3,"node_type":"array","shape":[4],"data_type":"int32","chunk_grid":{"name":"regular","configuration":{"chunk_shape":[4]}},"chunk_key_encoding":{"name":"default","configuration":{"separator":"/"}},"fill_value":0,"codecs":[{"name":"transpose","configuration":{"order":[0]}},{"name":"bytes","configuration":{"endian":"little"}},{"name":"gzip","configuration":{"level":6}}]}"""
    md = Zarr.Metadata(json_str, false)
    @test md isa Zarr.MetadataV3
    @test md.shape[] == (4,)
    @test md.chunks == (4,)
    @test md.fill_value == Int32(0)

    pipeline = Zarr.get_pipeline(md)
    @test pipeline isa Zarr.V3Pipeline
    @test length(pipeline.array_array) == 1
    @test pipeline.array_bytes isa Zarr.Codecs.V3Codecs.BytesCodec
    @test length(pipeline.bytes_bytes) == 1
end

@testset "V3 Metadata JSON round-trip" begin
    json_str = """{"zarr_format":3,"node_type":"array","shape":[4,4],"data_type":"float64","chunk_grid":{"name":"regular","configuration":{"chunk_shape":[2,2]}},"chunk_key_encoding":{"name":"default","configuration":{"separator":"/"}},"fill_value":0.0,"codecs":[{"name":"bytes","configuration":{"endian":"little"}},{"name":"blosc","configuration":{"cname":"lz4","clevel":5,"shuffle":"noshuffle","blocksize":0,"typesize":4}}]}"""
    md = Zarr.Metadata(json_str, false)
    @test md isa Zarr.MetadataV3

    # Serialize back to JSON
    lowered = JSON.lower(md)
    @test lowered["zarr_format"] == 3
    @test lowered["codecs"][1]["name"] == "bytes"
    @test lowered["codecs"][2]["name"] == "blosc"
end

@testset "V3 Group Metadata Parsing" begin
    json_str = """{"zarr_format":3,"node_type":"group"}"""
    md = Zarr.Metadata(json_str, false)
    @test md isa Zarr.MetadataV3
    @test md.node_type == "group"
end

@testset "typestr3 raw types" begin
    @test Zarr.typestr3("r8")  == NTuple{1,UInt8}
    @test Zarr.typestr3("r16") == NTuple{2,UInt8}
    @test Zarr.typestr3("r64") == NTuple{8,UInt8}
    @test_throws ArgumentError Zarr.typestr3("rxyz")   # non-numeric bits
    @test_throws ArgumentError Zarr.typestr3("r7")     # not a multiple of 8
end

@testset "V3 Metadata parsing error paths" begin
    base = """{"zarr_format":3,"node_type":"array","shape":[4],"data_type":"int32",
        "chunk_grid":{"name":"regular","configuration":{"chunk_shape":[4]}},
        "chunk_key_encoding":{"name":"default","configuration":{"separator":"/"}},
        "fill_value":0,"codecs":[{"name":"bytes","configuration":{"endian":"little"}}]}"""

    # Unknown node_type
    @test_throws ArgumentError Zarr.Metadata("""{"zarr_format":3,"node_type":"unknown"}""", false)

    # Extra key in group metadata
    @test_throws ArgumentError Zarr.Metadata("""{"zarr_format":3,"node_type":"group","bad_key":1}""", false)

    # Missing required key (shape)
    @test_throws ArgumentError Zarr.Metadata("""{"zarr_format":3,"node_type":"array","data_type":"int32",
        "chunk_grid":{"name":"regular","configuration":{"chunk_shape":[4]}},
        "chunk_key_encoding":{"name":"default","configuration":{"separator":"/"}},
        "fill_value":0,"codecs":[{"name":"bytes","configuration":{"endian":"little"}}]}""", false)

    # Unknown chunk_grid name
    @test_throws ArgumentError Zarr.Metadata("""{"zarr_format":3,"node_type":"array","shape":[4],"data_type":"int32",
        "chunk_grid":{"name":"unknown","configuration":{"chunk_shape":[4]}},
        "chunk_key_encoding":{"name":"default","configuration":{"separator":"/"}},
        "fill_value":0,"codecs":[{"name":"bytes","configuration":{"endian":"little"}}]}""", false)

    # Shape/chunk rank mismatch
    @test_throws ArgumentError Zarr.Metadata("""{"zarr_format":3,"node_type":"array","shape":[4],"data_type":"int32",
        "chunk_grid":{"name":"regular","configuration":{"chunk_shape":[2,2]}},
        "chunk_key_encoding":{"name":"default","configuration":{"separator":"/"}},
        "fill_value":0,"codecs":[{"name":"bytes","configuration":{"endian":"little"}}]}""", false)

    # Unknown chunk_key_encoding name
    @test_throws ArgumentError Zarr.Metadata("""{"zarr_format":3,"node_type":"array","shape":[4],"data_type":"int32",
        "chunk_grid":{"name":"regular","configuration":{"chunk_shape":[4]}},
        "chunk_key_encoding":{"name":"unknown"},
        "fill_value":0,"codecs":[{"name":"bytes","configuration":{"endian":"little"}}]}""", false)

    # Unknown codec
    @test_throws ArgumentError Zarr.Metadata("""{"zarr_format":3,"node_type":"array","shape":[4],"data_type":"int32",
        "chunk_grid":{"name":"regular","configuration":{"chunk_shape":[4]}},
        "chunk_key_encoding":{"name":"default","configuration":{"separator":"/"}},
        "fill_value":0,"codecs":[{"name":"bytes","configuration":{"endian":"little"}},{"name":"unknown_codec"}]}""", false)

    # Deprecated string transpose order "C"
    @test_logs (:warn,) Zarr.Metadata("""{"zarr_format":3,"node_type":"array","shape":[4],"data_type":"int32",
        "chunk_grid":{"name":"regular","configuration":{"chunk_shape":[4]}},
        "chunk_key_encoding":{"name":"default","configuration":{"separator":"/"}},
        "fill_value":0,"codecs":[{"name":"transpose","configuration":{"order":"C"}},
        {"name":"bytes","configuration":{"endian":"little"}}]}""", false)

    # Deprecated string transpose order "F"
    @test_logs (:warn,) Zarr.Metadata("""{"zarr_format":3,"node_type":"array","shape":[4],"data_type":"int32",
        "chunk_grid":{"name":"regular","configuration":{"chunk_shape":[4]}},
        "chunk_key_encoding":{"name":"default","configuration":{"separator":"/"}},
        "fill_value":0,"codecs":[{"name":"transpose","configuration":{"order":"F"}},
        {"name":"bytes","configuration":{"endian":"little"}}]}""", false)

    # Unknown string transpose order
    @test_throws ArgumentError Zarr.Metadata("""{"zarr_format":3,"node_type":"array","shape":[4],"data_type":"int32",
        "chunk_grid":{"name":"regular","configuration":{"chunk_shape":[4]}},
        "chunk_key_encoding":{"name":"default","configuration":{"separator":"/"}},
        "fill_value":0,"codecs":[{"name":"transpose","configuration":{"order":"X"}},
        {"name":"bytes","configuration":{"endian":"little"}}]}""", false)
end

@testset "V3 Metadata parsing extended codecs" begin
    # zstd codec parses correctly
    json_zstd = """{"zarr_format":3,"node_type":"array","shape":[4],"data_type":"int32",
        "chunk_grid":{"name":"regular","configuration":{"chunk_shape":[4]}},
        "chunk_key_encoding":{"name":"default","configuration":{"separator":"/"}},
        "fill_value":0,"codecs":[{"name":"bytes","configuration":{"endian":"little"}},
        {"name":"zstd","configuration":{"level":3}}]}"""
    md = Zarr.Metadata(json_zstd, false)
    pipeline = Zarr.get_pipeline(md)
    @test pipeline.bytes_bytes[1] isa Zarr.Codecs.V3Codecs.ZstdV3Codec
    @test pipeline.bytes_bytes[1].level == 3

    # crc32c codec parses correctly
    json_crc = """{"zarr_format":3,"node_type":"array","shape":[4],"data_type":"int32",
        "chunk_grid":{"name":"regular","configuration":{"chunk_shape":[4]}},
        "chunk_key_encoding":{"name":"default","configuration":{"separator":"/"}},
        "fill_value":0,"codecs":[{"name":"bytes","configuration":{"endian":"little"}},
        {"name":"crc32c"}]}"""
    md = Zarr.Metadata(json_crc, false)
    pipeline = Zarr.get_pipeline(md)
    @test pipeline.bytes_bytes[1] isa Zarr.Codecs.V3Codecs.CRC32cV3Codec

    # F-order from numeric reverse permutation sets order='F'
    json_f = """{"zarr_format":3,"node_type":"array","shape":[3,4],"data_type":"int32",
        "chunk_grid":{"name":"regular","configuration":{"chunk_shape":[3,4]}},
        "chunk_key_encoding":{"name":"default","configuration":{"separator":"/"}},
        "fill_value":0,"codecs":[{"name":"transpose","configuration":{"order":[1,0]}},
        {"name":"bytes","configuration":{"endian":"little"}}]}"""
    md = Zarr.Metadata(json_f, false)
    @test Zarr.get_order(md) == 'F'

    # v2 chunk_key_encoding (prefix=false, separator='.')
    json_v2enc = """{"zarr_format":3,"node_type":"array","shape":[4],"data_type":"int32",
        "chunk_grid":{"name":"regular","configuration":{"chunk_shape":[4]}},
        "chunk_key_encoding":{"name":"v2","configuration":{"separator":"."}},
        "fill_value":0,"codecs":[{"name":"bytes","configuration":{"endian":"little"}}]}"""
    md = Zarr.Metadata(json_v2enc, false)
    @test md.chunk_key_encoding.prefix == false
    @test md.chunk_key_encoding.sep == '.'
end

@testset "SuffixChunkKeyEncoding" begin
    # Parsing: suffix encoding with default base
    json_str = """{"zarr_format":3,"node_type":"array","shape":[4,4],"data_type":"int32",
        "chunk_grid":{"name":"regular","configuration":{"chunk_shape":[2,2]}},
        "chunk_key_encoding":{"name":"suffix","configuration":{
            "suffix":".tiff",
            "base_encoding":{"name":"default"}
        }},
        "fill_value":0,"codecs":[{"name":"bytes","configuration":{"endian":"little"}}]}"""
    md = Zarr.Metadata(json_str, false)
    @test md.chunk_key_encoding isa Zarr.SuffixChunkKeyEncoding
    @test md.chunk_key_encoding.suffix == ".tiff"
    @test md.chunk_key_encoding.base_encoding isa Zarr.ChunkKeyEncoding
    @test md.chunk_key_encoding.base_encoding.prefix == true   # "default" uses c/ prefix
    @test md.chunk_key_encoding.base_encoding.sep == '/'

    # citostring appends suffix to base key
    e = md.chunk_key_encoding
    @test Zarr.citostring(e, CartesianIndex(1, 1)) == "c/0/0.tiff"
    @test Zarr.citostring(e, CartesianIndex(2, 1)) == "c/0/1.tiff"
    @test Zarr.citostring(e, CartesianIndex(1, 2)) == "c/1/0.tiff"

    # Parsing: suffix encoding with v2 base
    json_v2base = """{"zarr_format":3,"node_type":"array","shape":[4,4],"data_type":"int32",
        "chunk_grid":{"name":"regular","configuration":{"chunk_shape":[2,2]}},
        "chunk_key_encoding":{"name":"suffix","configuration":{
            "suffix":".shard.zip",
            "base_encoding":{"name":"v2"}
        }},
        "fill_value":0,"codecs":[{"name":"bytes","configuration":{"endian":"little"}}]}"""
    md2 = Zarr.Metadata(json_v2base, false)
    @test md2.chunk_key_encoding.suffix == ".shard.zip"
    @test md2.chunk_key_encoding.base_encoding.prefix == false  # "v2" has no prefix
    @test Zarr.citostring(md2.chunk_key_encoding, CartesianIndex(1, 1)) == "0.0.shard.zip"

    # Serialization round-trip
    lowered = JSON.lower(md)
    cke = lowered["chunk_key_encoding"]
    @test cke["name"] == "suffix"
    @test cke["configuration"]["suffix"] == ".tiff"
    @test cke["configuration"]["base_encoding"]["name"] == "default"

    # ZArray round-trip: chunks are stored with the suffix in their keys
    store = Zarr.DictStore()
    cke = Zarr.SuffixChunkKeyEncoding(".tiff", Zarr.ChunkKeyEncoding('/', true))
    bytes_codec = Zarr.Codecs.V3Codecs.BytesCodec()
    pipeline = Zarr.V3Pipeline((), bytes_codec, ())
    P = typeof(pipeline)
    E = typeof(cke)
    md = Zarr.MetadataV3{Int32,2,P,E}(3, "array", (4,4), (2,2), "int32", pipeline, Int32(0), cke)
    z = Zarr.ZArray(md, store, "", Dict(), true)
    z[:,:] = reshape(Int32.(1:16), 4, 4)
    @test z[:,:] == reshape(Int32.(1:16), 4, 4)
    @test any(k -> endswith(k, ".tiff"), keys(store.a))
end

@testset "V3 lower3 extended codecs" begin
    # ZstdV3Codec serialization
    json_zstd = """{"zarr_format":3,"node_type":"array","shape":[4],"data_type":"int32",
        "chunk_grid":{"name":"regular","configuration":{"chunk_shape":[4]}},
        "chunk_key_encoding":{"name":"default","configuration":{"separator":"/"}},
        "fill_value":0,"codecs":[{"name":"bytes","configuration":{"endian":"little"}},
        {"name":"zstd","configuration":{"level":5}}]}"""
    md = Zarr.Metadata(json_zstd, false)
    lowered = JSON.lower(md)
    @test lowered["codecs"][2]["name"] == "zstd"
    @test lowered["codecs"][2]["configuration"]["level"] == 5

    # CRC32cV3Codec serialization
    json_crc = """{"zarr_format":3,"node_type":"array","shape":[4],"data_type":"int32",
        "chunk_grid":{"name":"regular","configuration":{"chunk_shape":[4]}},
        "chunk_key_encoding":{"name":"default","configuration":{"separator":"/"}},
        "fill_value":0,"codecs":[{"name":"bytes","configuration":{"endian":"little"}},
        {"name":"crc32c"}]}"""
    md = Zarr.Metadata(json_crc, false)
    lowered = JSON.lower(md)
    @test lowered["codecs"][2]["name"] == "crc32c"

    # TransposeCodec serialization
    json_trans = """{"zarr_format":3,"node_type":"array","shape":[3,4],"data_type":"int32",
        "chunk_grid":{"name":"regular","configuration":{"chunk_shape":[3,4]}},
        "chunk_key_encoding":{"name":"default","configuration":{"separator":"/"}},
        "fill_value":0,"codecs":[{"name":"transpose","configuration":{"order":[1,0]}},
        {"name":"bytes","configuration":{"endian":"little"}}]}"""
    md = Zarr.Metadata(json_trans, false)
    lowered = JSON.lower(md)
    @test lowered["codecs"][1]["name"] == "transpose"
    @test lowered["codecs"][1]["configuration"]["order"] == [1, 0]
end

@testset "MetadataV3 convenience constructor" begin
    # order='F' creates a TransposeCodec
    data = zeros(Int32, 4, 4)
    md = Zarr.Metadata3(data, (4,4); order='F')
    @test Zarr.get_order(md) == 'F'
    pipeline = Zarr.get_pipeline(md)
    @test length(pipeline.array_array) == 1
    @test pipeline.array_array[1] isa Zarr.Codecs.V3Codecs.TransposeCodec

    # ZstdCompressor translates to ZstdV3Codec
    md_zstd = Zarr.Metadata3(data, (4,4); compressor=Zarr.ZstdCompressor())
    pipeline_zstd = Zarr.get_pipeline(md_zstd)
    @test pipeline_zstd.bytes_bytes[1] isa Zarr.Codecs.V3Codecs.ZstdV3Codec

    # fill_value=nothing defaults to zero(T)
    md_nofv = Zarr.Metadata3(data, (4,4))
    @test md_nofv.fill_value == Int32(0)

    # Unsupported compressor throws ArgumentError
    struct _BadCompressor <: Zarr.Compressor end
    @test_throws ArgumentError Zarr.Metadata3(data, (4,4); compressor=_BadCompressor())
end

@testset "V3 ZArray round-trip" begin
    z = zcreate(Int32, 8; zarr_format=3, chunks=(4,), fill_value=Int32(0))
    z[:] = Int32.(1:8)
    @test z[:] == Int32.(1:8)
    @test z[3:6] == Int32[3, 4, 5, 6]
end

@testset "V3 ZArray with gzip" begin
    z = zcreate(Float64, 4, 4; zarr_format=3, chunks=(2, 2),
        compressor=Zarr.ZlibCompressor(), fill_value=0.0)
    z[:, :] = reshape(Float64.(1:16), 4, 4)
    @test z[:, :] == reshape(Float64.(1:16), 4, 4)
end

@testset "V3 ZArray with blosc" begin
    z = zcreate(Int64, 10; zarr_format=3, chunks=(5,),
        compressor=Zarr.BloscCompressor(), fill_value=Int64(0))
    z[:] = Int64.(1:10)
    @test z[:] == Int64.(1:10)
end

@testset "V3 ZArray no compressor" begin
    z = zcreate(Float32, 6; zarr_format=3, chunks=(3,),
        compressor=Zarr.NoCompressor(), fill_value=Float32(0))
    z[:] = Float32.(1:6)
    @test z[:] == Float32.(1:6)
end

@testset "V3 Group Creation" begin
    store = Zarr.DictStore()
    g = zgroup(store, "", Zarr.ZarrFormat(3))
    @test haskey(store, "zarr.json")
    md = JSON.parse(String(copy(store["zarr.json"])))
    @test md["zarr_format"] == 3
    @test md["node_type"] == "group"

    # Create subgroup
    g2 = zgroup(g, "sub"; attrs=Dict("key" => "val"))
    md2 = JSON.parse(String(copy(store["sub/zarr.json"])))
    @test md2["zarr_format"] == 3
    @test md2["node_type"] == "group"
    @test md2["attributes"]["key"] == "val"
end

@testset "Read Julia-generated v3 fixtures" begin
    fixture_path = joinpath(@__DIR__, "v3_julia", "data.zarr")
    if isdir(fixture_path)
        store = Zarr.DirectoryStore(fixture_path)
        # Read a simple 1d array
        z = zopen(store; path="1d.contiguous.raw.i2")
        @test z[:] == Int16[1, 2, 3, 4]

        # Read a chunked 2d array
        z2 = zopen(store; path="2d.chunked.i2")
        @test z2[:, :] == Int16[1 2; 3 4]
    else
        @warn "v3 fixtures not found at $fixture_path, skipping"
    end
end

@testset "Read Julia-generated v3 fixtures with Python zarr" begin
    fixture_path = joinpath(@__DIR__, "v3_julia", "data.zarr")
    if !isdir(fixture_path)
        @warn "Julia v3 fixtures not found at $fixture_path, skipping"
    else
        using PythonCall
        np   = pyimport("numpy")
        zarr = pyimport("zarr")

        g = zarr.open_group(fixture_path, mode="r")

        @testset "1D arrays" begin
            @test pyconvert(Vector{Int16},   np.array(g["1d.contiguous.gzip.i2"]))  == Int16[1, 2, 3, 4]
            @test pyconvert(Vector{Int16},   np.array(g["1d.contiguous.blosc.i2"])) == Int16[1, 2, 3, 4]
            @test pyconvert(Vector{Int16},   np.array(g["1d.contiguous.raw.i2"]))   == Int16[1, 2, 3, 4]
            @test pyconvert(Vector{Int32},   np.array(g["1d.contiguous.i4"]))       == Int32[1, 2, 3, 4]
            @test pyconvert(Vector{UInt8},   np.array(g["1d.contiguous.u1"]))       == UInt8[255, 0, 255, 0]
            @test pyconvert(Vector{Float16}, np.array(g["1d.contiguous.f2.le"]))    == Float16[-1000.5, 0.0, 1000.5, 0.0]
            @test pyconvert(Vector{Float32}, np.array(g["1d.contiguous.f4.le"]))    == Float32[-1000.5, 0.0, 1000.5, 0.0]
            @test pyconvert(Vector{Float64}, np.array(g["1d.contiguous.f8"]))       == Float64[1.5, 2.5, 3.5, 4.5]
            @test pyconvert(Vector{Bool},    np.array(g["1d.contiguous.b1"]))       == Bool[true, false, true, false]
            @test pyconvert(Vector{Int16},   np.array(g["1d.chunked.i2"]))          == Int16[1, 2, 3, 4]
            @test pyconvert(Vector{Int16},   np.array(g["1d.chunked.ragged.i2"]))   == Int16[1, 2, 3, 4, 5]
        end

        @testset "2D arrays" begin
            # Julia column-major [1 2; 3 4] → Python row-major [[1,3],[2,4]]
            arr2d = pyconvert(Matrix{Int16}, np.array(g["2d.contiguous.i2"]))
            @test arr2d == Int16[1 3; 2 4]

            arr2d_chunked = pyconvert(Matrix{Int16}, np.array(g["2d.chunked.i2"]))
            @test arr2d_chunked == Int16[1 3; 2 4]
        end

        @testset "3D arrays" begin
            # Julia writes reshape(Int16.(0:26), 3,3,3) in column-major order.
            # Python reads the zarr shape [3,3,3] in C (row-major) order, so
            # pyconvert maps Python[i,j,k] → Julia[i+1,j+1,k+1], yielding
            # permutedims(reshape(Int16.(0:26),3,3,3), (3,2,1)).
            arr3d = pyconvert(Array{Int16,3}, np.array(g["3d.contiguous.i2"]))
            @test arr3d == permutedims(reshape(Int16.(0:26), 3, 3, 3), (3, 2, 1))
        end

        @testset "Sharded 1D arrays" begin
            @test pyconvert(Vector{Int16},   np.array(g["1d.contiguous.compressed.sharded.i2"])) == Int16[1, 2, 3, 4]
            @test pyconvert(Vector{Int32},   np.array(g["1d.contiguous.compressed.sharded.i4"])) == Int32[1, 2, 3, 4]
            @test pyconvert(Vector{UInt8},   np.array(g["1d.contiguous.compressed.sharded.u1"])) == UInt8[255, 0, 255, 0]
            @test pyconvert(Vector{Float32}, np.array(g["1d.contiguous.compressed.sharded.f4"])) == Float32[-1000.5, 0, 1000.5, 0]
            @test pyconvert(Vector{Float64}, np.array(g["1d.contiguous.compressed.sharded.f8"])) == Float64[1.5, 2.5, 3.5, 4.5]
            @test pyconvert(Vector{Bool},    np.array(g["1d.contiguous.compressed.sharded.b1"])) == Bool[true, false, true, false]
            @test pyconvert(Vector{Int16},   np.array(g["1d.chunked.compressed.sharded.i2"]))    == Int16[1, 2, 3, 4]
            @test pyconvert(Vector{Int16},   np.array(g["1d.chunked.filled.compressed.sharded.i2"])) == Int16[1, 2, 0, 0]
        end

        @testset "Group with spaces in name" begin
            desc = pyconvert(String, g["my group with spaces"].attrs["description"])
            @test desc == "A group with spaces in the name"
        end
    end
end

@testset "V3 Integration" begin
    @testset "zzeros with v3" begin
        z = zzeros(Float32, 10, 10; zarr_format=3, chunks=(5, 5), fill_value=Float32(0))
        @test size(z) == (10, 10)
        @test all(==(0.0f0), z[:, :])
    end

    @testset "V3 zopen round-trip with DirectoryStore" begin
        mktempdir() do dir
            path = joinpath(dir, "test.zarr")
            z = zcreate(Int64, 4, 4; path=path, zarr_format=3,
                chunks=(2, 2), fill_value=Int64(0))
            z[:, :] = reshape(Int64.(1:16), 4, 4)

            z2 = zopen(path)
            @test z2[:, :] == reshape(Int64.(1:16), 4, 4)
        end
    end

    @testset "V3 group with arrays" begin
        store = Zarr.DictStore()
        g = zgroup(store, "", Zarr.ZarrFormat(3))
        a = zcreate(Float64, g, "myarray", 10; zarr_format=3,
            chunks=(5,), fill_value=0.0)
        a[:] = Float64.(1:10)

        g2 = zopen(store)
        @test g2["myarray"][:] == Float64.(1:10)
    end
end

@testset "Read Python-generated v3 fixtures" begin
    fixture_path = joinpath(@__DIR__, "v3_python", "data.zarr")
    if !isdir(fixture_path)
        @warn "Python v3 fixtures not found at $fixture_path, skipping"
    else
        store = Zarr.DirectoryStore(fixture_path)

        @testset "1D contiguous arrays" begin
            # gzip compressed
            z = zopen(store; path="1d.contiguous.gzip.i2")
            @test eltype(z) == Int16
            @test size(z) == (4,)
            @test z[:] == Int16[1, 2, 3, 4]

            # blosc compressed
            z = zopen(store; path="1d.contiguous.blosc.i2")
            @test z[:] == Int16[1, 2, 3, 4]

            # "raw" — actually zstd in modern Python zarr v3
            z = zopen(store; path="1d.contiguous.raw.i2")
            @test z[:] == Int16[1, 2, 3, 4]

            # Int32
            z = zopen(store; path="1d.contiguous.i4")
            @test eltype(z) == Int32
            @test z[:] == Int32[1, 2, 3, 4]

            # UInt8
            z = zopen(store; path="1d.contiguous.u1")
            @test eltype(z) == UInt8
            @test z[:] == UInt8[255, 0, 255, 0]

            # Float16 little-endian
            z = zopen(store; path="1d.contiguous.f2.le")
            @test eltype(z) == Float16
            @test z[:] == Float16[-1000.5, 0.0, 1000.5, 0.0]

            # Float32 little-endian
            z = zopen(store; path="1d.contiguous.f4.le")
            @test eltype(z) == Float32
            @test z[:] == Float32[-1000.5, 0.0, 1000.5, 0.0]

            # Float64
            z = zopen(store; path="1d.contiguous.f8")
            @test eltype(z) == Float64
            @test z[:] == Float64[1.5, 2.5, 3.5, 4.5]

            # Bool
            z = zopen(store; path="1d.contiguous.b1")
            @test eltype(z) == Bool
            @test z[:] == Bool[true, false, true, false]
        end

        @testset "1D chunked arrays" begin
            z = zopen(store; path="1d.chunked.i2")
            @test size(z) == (4,)
            @test z[:] == Int16[1, 2, 3, 4]

            # Ragged: shape (5,) with chunks (2,) — last chunk is partial
            z = zopen(store; path="1d.chunked.ragged.i2")
            @test size(z) == (5,)
            @test z[:] == Int16[1, 2, 3, 4, 5]
        end

        @testset "2D arrays" begin
            # Python [[1,2],[3,4]] row-major -> Julia [1 3; 2 4] column-major
            z = zopen(store; path="2d.contiguous.i2")
            @test size(z) == (2, 2)
            @test z[:, :] == Int16[1 3; 2 4]

            # 2D chunked with (1,1) chunks
            z = zopen(store; path="2d.chunked.i2")
            @test size(z) == (2, 2)
            @test z[:, :] == Int16[1 3; 2 4]

            # 2D chunked ragged: Python [[1,2,3],[4,5,6],[7,8,9]] (3x3, chunks 2x2)
            z = zopen(store; path="2d.chunked.ragged.i2")
            @test size(z) == (3, 3)
            @test z[:, :] == Int16[1 4 7; 2 5 8; 3 6 9]
        end

        @testset "3D arrays" begin
            # Python np.arange(27).reshape(3,3,3) in C order
            # In Julia column-major: reshape(Int16.(0:26), 3, 3, 3)
            expected_3d = reshape(Int16.(0:26), 3, 3, 3)

            z = zopen(store; path="3d.contiguous.i2")
            @test size(z) == (3, 3, 3)
            @test z[:, :, :] == expected_3d

            # Chunked with (1,1,1) chunks — same data, different chunking
            z = zopen(store; path="3d.chunked.i2")
            @test size(z) == (3, 3, 3)
            @test z[:, :, :] == expected_3d

            # Mixed chunking (3,3,1) in Julia (reversed from Python's (1,3,3))
            z = zopen(store; path="3d.chunked.mixed.i2.C")
            @test size(z) == (3, 3, 3)
            @test z[:, :, :] == expected_3d
        end

        @testset "3D with transpose codec (F-order)" begin
            # Same data as 3d.chunked.mixed.i2.C but with transpose([2,1,0]) codec
            expected_3d = reshape(Int16.(0:26), 3, 3, 3)
            z = zopen(store; path="3d.chunked.mixed.i2.F")
            @test size(z) == (3, 3, 3)
            @test z[:, :, :] == expected_3d
        end

        @testset "Big-endian float32" begin
            z = zopen(store; path="1d.contiguous.f4.be")
            @test eltype(z) == Float32
            @test z[:] == Float32[-1000.5, 0.0, 1000.5, 0.0]
        end

        @testset "Sharded 1D array" begin
            z = zopen(store; path="1d.contiguous.compressed.sharded.i2")
            @test eltype(z) == Int16
            @test size(z) == (4,)
            @test z[:] == Int16[1, 2, 3, 4]
        end

        @testset "Group with spaces in name" begin
            g = zopen(store)
            @test haskey(g, "my group with spaces")
            sub = g["my group with spaces"]
            @test sub isa Zarr.ZGroup
            @test sub.attrs["description"] == "A group with spaces in the name"
        end
    end
end

@testset "ShardingCodec round-trip" begin
    # shard shape (4,), inner chunk shape (2,), bytes + gzip inside, bytes + crc32c for index
    c = Zarr.Codecs.V3Codecs.getCodec(Dict(
        "name" => "sharding_indexed",
        "configuration" => Dict(
            "chunk_shape"    => [2],
            "codecs"         => [Dict("name"=>"bytes","configuration"=>Dict("endian"=>"little")),
                                 Dict("name"=>"gzip","configuration"=>Dict("level"=>6))],
            "index_codecs"   => [Dict("name"=>"bytes","configuration"=>Dict("endian"=>"little")),
                                 Dict("name"=>"crc32c")],
            "index_location" => "end",
        )
    ))

    data = Int16[1, 2, 3, 4]
    encoded = Zarr.Codecs.V3Codecs.codec_encode(c, data)
    @test encoded isa Vector{UInt8}
    @test !isempty(encoded)

    decoded = Zarr.Codecs.V3Codecs.codec_decode(c, encoded, Int16, (4,))
    @test decoded == reshape(data, 4)

    # JSON round-trip
    lowered = JSON.lower(c)
    @test lowered["name"] == "sharding_indexed"
    @test lowered["configuration"]["chunk_shape"] == [2]
    @test lowered["configuration"]["index_location"] == "end"
end

@testset "ShardingCodec ragged inner chunks" begin
    # Outer chunk (shard) size does not evenly divide by inner chunk size.
    # shard shape (3,), inner chunk shape (2,): 2 inner chunks — full (1:2) + partial (3:3)
    inner_pipeline = Zarr.V3Pipeline(
        (),
        Zarr.Codecs.V3Codecs.BytesCodec(:little),
        (Zarr.Codecs.V3Codecs.CRC32cV3Codec(),)
    )
    index_pipeline = Zarr.V3Pipeline(
        (),
        Zarr.Codecs.V3Codecs.BytesCodec(:little),
        (Zarr.Codecs.V3Codecs.CRC32cV3Codec(),)
    )
    sharding = Zarr.Codecs.V3Codecs.ShardingCodec((2,), inner_pipeline, index_pipeline, :end)
    pipeline = Zarr.V3Pipeline((), sharding, ())
    md = Zarr.MetadataV3{Int16,1,typeof(pipeline)}(
        3, "array", (3,), (3,), "int16", pipeline, Int16(0),
        Zarr.ChunkKeyEncoding('/', true)
    )
    store = Zarr.DictStore()
    z = Zarr.ZArray(md, store, "", Dict(), true)

    data = Int16[10, 20, 30]
    z[:] = data
    @test z[:] == data

    # 2D: shard (3,3), inner (2,2) — partial chunks on both axes
    inner_pipeline2 = Zarr.V3Pipeline(
        (),
        Zarr.Codecs.V3Codecs.BytesCodec(:little),
        ()
    )
    index_pipeline2 = Zarr.V3Pipeline(
        (),
        Zarr.Codecs.V3Codecs.BytesCodec(:little),
        (Zarr.Codecs.V3Codecs.CRC32cV3Codec(),)
    )
    sharding2 = Zarr.Codecs.V3Codecs.ShardingCodec((2,2), inner_pipeline2, index_pipeline2, :end)
    pipeline2 = Zarr.V3Pipeline((), sharding2, ())
    md2 = Zarr.MetadataV3{Int32,2,typeof(pipeline2)}(
        3, "array", (3,3), (3,3), "int32", pipeline2, Int32(0),
        Zarr.ChunkKeyEncoding('/', true)
    )
    store2 = Zarr.DictStore()
    z2 = Zarr.ZArray(md2, store2, "", Dict(), true)

    data2 = reshape(Int32.(1:9), 3, 3)
    z2[:,:] = data2
    @test z2[:,:] == data2
end

@testset "ShardingCodec ZArray write and read" begin
    # Build a pipeline where ShardingCodec is the array->bytes codec.
    # Shard shape (outer chunk): (4,). Inner chunk shape: (2,).
    inner_pipeline = Zarr.V3Pipeline(
        (),
        Zarr.Codecs.V3Codecs.BytesCodec(:little),
        (Zarr.Codecs.V3Codecs.GzipV3Codec(6),)
    )
    index_pipeline = Zarr.V3Pipeline(
        (),
        Zarr.Codecs.V3Codecs.BytesCodec(:little),
        (Zarr.Codecs.V3Codecs.CRC32cV3Codec(),)
    )
    sharding = Zarr.Codecs.V3Codecs.ShardingCodec((2,), inner_pipeline, index_pipeline, :end)
    pipeline = Zarr.V3Pipeline((), sharding, ())
    md = Zarr.MetadataV3{Int16,1,typeof(pipeline)}(
        3, "array", (4,), (4,), "int16", pipeline, Int16(0),
        Zarr.ChunkKeyEncoding('/', true)
    )
    store = Zarr.DictStore()
    z = Zarr.ZArray(md, store, "", Dict(), true)

    data = Int16[1, 2, 3, 4]
    z[:] = data
    @test z[:] == data
end

@testset "ShardingCodec non-zero fill_value" begin
    # Shard shape (4,), inner chunk (2,); only write to first inner chunk.
    # The second inner chunk should read back as fill_value (Int16(99)), not zero.
    inner_pipeline = Zarr.V3Pipeline(
        (),
        Zarr.Codecs.V3Codecs.BytesCodec(:little),
        ()
    )
    index_pipeline = Zarr.V3Pipeline(
        (),
        Zarr.Codecs.V3Codecs.BytesCodec(:little),
        (Zarr.Codecs.V3Codecs.CRC32cV3Codec(),)
    )
    sharding = Zarr.Codecs.V3Codecs.ShardingCodec((2,), inner_pipeline, index_pipeline, :end)
    pipeline = Zarr.V3Pipeline((), sharding, ())
    md = Zarr.MetadataV3{Int16,1,typeof(pipeline)}(
        3, "array", (4,), (4,), "int16", pipeline, Int16(99),
        Zarr.ChunkKeyEncoding('/', true)
    )
    store = Zarr.DictStore()
    z = Zarr.ZArray(md, store, "", Dict(), true)

    # Write only the first two elements; the shard is written as one outer chunk.
    # The second inner chunk is never written so it should decode to fill_value.
    z[1:2] = Int16[10, 20]
    @test z[1:2] == Int16[10, 20]
    @test z[3:4] == Int16[99, 99]
end

end # V3 Codecs
