using Test
using ZarrCore
using JSON

@testset "ZarrCore" begin
    @testset "NoCompressor roundtrip" begin
        z = zzeros(Int64, 4, 4)
        @test z isa ZArray
        @test size(z) == (4, 4)
        @test z[1,1] == 0
        z[2,3] = 42
        @test z[2,3] == 42
    end

    @testset "DirectoryStore" begin
        p = mktempdir()
        z = zcreate(Float64, 10, 10; path=p, chunks=(5,5))
        z[:] = reshape(1.0:100.0, 10, 10)
        z2 = zopen(p)
        @test z2[1,1] == 1.0
        @test z2[10,10] == 100.0
    end

    @testset "DictStore" begin
        z = zcreate(Int32, 6, 6; chunks=(3,3))
        z[:] = ones(Int32, 6, 6)
        @test z[1,1] == 1
    end

    @testset "V3 uncompressed roundtrip" begin
        z = zcreate(Float32, 8, 8;
            zarr_format=3,
            chunks=(4,4),
            compressor=ZarrCore.NoCompressor())
        z[:] = reshape(Float32.(1:64), 8, 8)
        @test z[1,1] == 1.0f0
        @test z[8,8] == 64.0f0
    end

    @testset "default_compressor is NoCompressor" begin
        @test ZarrCore.default_compressor() isa ZarrCore.NoCompressor
    end
end
