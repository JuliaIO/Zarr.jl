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

end # V3 Codecs
