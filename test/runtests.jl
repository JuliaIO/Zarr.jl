using Test
using ZarrNative
using JSON

@testset "ZarrNative" begin

@testset "ZArray" begin
    @testset "fields" begin
        z = zzeros(Int, 2, 3)
        @test z isa ZArray{Int, 2, ZarrNative.BloscCompressor,
            ZarrNative.DictStore{Matrix{Vector{UInt8}}}}

        @test z.storage.name === "data"
        @test length(z.storage.a) === 1
        @test length(z.storage.a[1]) === 64
        @test eltype(z.storage.a[1]) === UInt8
        @test z.metadata.shape === (2, 3)
        @test z.metadata.order === 'C'
        @test z.metadata.chunks === (2, 3)
        @test z.metadata.fill_value === nothing
        @test z.metadata.compressor isa ZarrNative.BloscCompressor
        @test z.metadata.compressor.blocksize === 0
        @test z.metadata.compressor.clevel === 5
        @test z.metadata.compressor.cname === "lz4"
        @test z.metadata.compressor.shuffle === true
        @test z.attrs == Dict{Any, Any}()
        @test z.writeable === true
    end

    @testset "methods" begin
        z = zzeros(Int, 2, 3)
        @test z isa ZArray{Int, 2, ZarrNative.BloscCompressor,
            ZarrNative.DictStore{Matrix{Vector{UInt8}}}}

        @test eltype(z) === Int
        @test ndims(z) === 2
        @test size(z) === (2, 3)
        @test size(z, 2) === 3
        @test length(z) === 2 * 3
        @test lastindex(z, 2) === 3
        @test ZarrNative.zname(z) === "data"
    end

    @testset "NoCompressor DirectoryStore" begin
        mktempdir(@__DIR__) do dir
            name = "nocompressor"
            z = zzeros(Int, 2, 3, path="$dir/$name",
                compressor=ZarrNative.NoCompressor())

            @test z.metadata.compressor === ZarrNative.NoCompressor()
            @test z.storage === ZarrNative.DirectoryStore("$dir/$name")
            @test isdir("$dir/$name")
            @test ispath("$dir/$name/.zarray")
            @test ispath("$dir/$name/.zattrs")
            @test ispath("$dir/$name/0.0")
            @test JSON.parsefile("$dir/$name/.zattrs") == Dict{String, Any}()
            @test JSON.parsefile("$dir/$name/.zarray") == Dict{String, Any}(
                "dtype" => "<i8",
                "filters" => nothing,
                "shape" => [3, 2],
                "order" => "C",
                "zarr_format" => 2,
                "chunks" => [3, 2],
                "fill_value" => nothing,
                "compressor" => nothing)
            # call gc to avoid unlink: operation not permitted (EPERM) on Windows
            # might be because files are left open
            # from https://github.com/JuliaLang/julia/blob/f6344d32d3ebb307e2b54a77e042559f42d2ebf6/stdlib/SharedArrays/test/runtests.jl#L146
            GC.gc()
        end
    end
end

@testset "Metadata" begin
    @testset "Data type encoding" begin
        @test ZarrNative.typestr(Bool) === "<b1"
        @test ZarrNative.typestr(Int8) === "<i1"
        @test ZarrNative.typestr(Int64) === "<i8"
        @test ZarrNative.typestr(UInt32) === "<u4"
        @test ZarrNative.typestr(UInt128) === "<u16"
        @test ZarrNative.typestr(Complex{Float32}) === "<c8"
        @test ZarrNative.typestr(Complex{Float64}) === "<c16"
        @test ZarrNative.typestr(Float16) === "<f2"
        @test ZarrNative.typestr(Float64) === "<f8"
    end

    @testset "Metadata struct and JSON representation" begin
        A = fill(1.0, 30, 20)
        chunks = (5,10)
        metadata = ZarrNative.Metadata(A, chunks; fill_value=-1.5)
        @test metadata isa ZarrNative.Metadata
        @test metadata.zarr_format === 2
        @test metadata.shape === size(A)
        @test metadata.chunks === chunks
        @test metadata.dtype === "<f8"
        @test metadata.compressor === ZarrNative.BloscCompressor(0, 5, "lz4", true)
        @test metadata.fill_value === -1.5
        @test metadata.order === 'C'
        @test metadata.filters === nothing

        jsonstr = json(metadata)
        metadata_cycled = ZarrNative.Metadata(jsonstr)
        @test metadata === metadata_cycled
    end

    @testset "Fill value" begin
        @test ZarrNative.fill_value_encoding(Inf) === "Infinity"
        @test ZarrNative.fill_value_encoding(-Inf) === "-Infinity"
        @test ZarrNative.fill_value_encoding(NaN) === "NaN"
        @test ZarrNative.fill_value_encoding(nothing) === nothing
        @test ZarrNative.fill_value_encoding("-") === "-"

        @test ZarrNative.fill_value_decoding("Infinity", Float64) === Inf
        @test ZarrNative.fill_value_decoding("-Infinity", Float64) === -Inf
        @test ZarrNative.fill_value_decoding("NaN", Float32) === NaN32
        @test ZarrNative.fill_value_decoding("3.4", Float64) === 3.4
        @test ZarrNative.fill_value_decoding("3", Int) === 3
        @test ZarrNative.fill_value_decoding(nothing, Int) === nothing
        @test ZarrNative.fill_value_decoding("-", String) === "-"
    end
end

@testset "Storage" begin
    mixed_path = ".\\\\path///to\\a\\place/..\\///"
    norm_path = "path/to/a"
    @test ZarrNative.normalize_path(mixed_path) == norm_path
    @test ZarrNative.DirectoryStore(mixed_path).folder == norm_path
end

end  # @testset "ZarrNative"
