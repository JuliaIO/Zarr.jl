using Zarr
using Test

@testset "Zarr Extension Packages" begin
    @test_throws Zarr.UnknownCompressorException("zstd") zzeros(UInt8, 512, compressor="zstd")
    @test_throws Zarr.UnknownCompressorException("asdf") zzeros(UInt8, 512, compressor="asdf")
    d = Dict("id" => "zstd")
    @test_throws Zarr.UnknownCompressorException("zstd") Zarr.getCompressor(d)

    iob = IOBuffer()
    show(iob, Zarr.UnknownCompressorException("zstd"))
    @test occursin("CodecZstd.jl", String(take!(iob)))

    iob = IOBuffer()
    show(iob, Zarr.UnknownCompressorException("asdf"))
    @test occursin("issue", String(take!(iob)))
    @test Zarr.getCompressor(nothing) == Zarr.NoCompressor()
end

using CodecZstd
@testset "Zarr CodecZstd Extension" begin
    CodecZstdExt = Base.get_extension(Zarr, :CodecZstdExt)
    @test haskey(Zarr.compressortypes, "zstd")
    @test Zarr.compressortypes["zstd"] == CodecZstdExt.ZstdZarrCompressor
    td = tempname()
    zarray = zzeros(UInt16, 16, 16, compressor="zstd", path=td)
    zarray .= reshape(1:256,16,16)
    @test isa(zarray, ZArray{UInt16})
    @test zarray.metadata.compressor isa CodecZstdExt.ZstdZarrCompressor
    zarray2 = zopen(td)
    @test isa(zarray2, ZArray{UInt16})
    @test zarray2.metadata.compressor isa CodecZstdExt.ZstdZarrCompressor
    @test zarray2 == reshape(1:256,16,16)
end
