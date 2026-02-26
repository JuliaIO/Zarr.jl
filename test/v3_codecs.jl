using Test
using Zarr

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

@testset "TransposeCodecImpl" begin
    codec_c = Zarr.Codecs.V3Codecs.TransposeCodecImpl((1, 2, 3))
    data = reshape(collect(1:24), 2, 3, 4)
    encoded = Zarr.Codecs.V3Codecs.codec_encode(codec_c, data)
    @test encoded == data

    codec_f = Zarr.Codecs.V3Codecs.TransposeCodecImpl((3, 2, 1))
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

end # V3 Codecs
