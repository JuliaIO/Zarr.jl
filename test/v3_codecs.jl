using Test
using Zarr
using JSON

@testset "V3 Codecs" begin

@testset "BytesCodec" begin
    codec = Zarr.Codecs.V3Codecs.BytesCodec()
    data = Int32[1, 2, 3, 4]
    encoded = Zarr.Codecs.V3Codecs.codec_encode(codec, data)
    @test encoded isa Vector{UInt8}
    @test length(encoded) == 16
    decoded = Zarr.Codecs.V3Codecs.codec_decode(codec, encoded, Int32, (4,))
    @test decoded == data
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

        @testset "Big-endian is rejected" begin
            # Big-endian bytes codec is not yet supported
            @test_throws ArgumentError zopen(store; path="1d.contiguous.f4.be")
        end

        @testset "Sharded arrays are rejected" begin
            # Sharding codec is not yet wired into the read pipeline
            @test_throws ArgumentError zopen(store; path="1d.contiguous.compressed.sharded.i2")
        end
    end
end

end # V3 Codecs
