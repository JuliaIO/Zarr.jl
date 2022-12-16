using Test
using Zarr
using JSON
using Pkg
using PyCall
using Dates

macro test_py(ex)
    quote
        @test convert(Bool, $(esc(ex)))
    end
end

@testset "Zarr" begin


@testset "ZArray" begin
    @testset "fields" begin
        z = zzeros(Int64, 2, 3)
        @test z isa ZArray{Int64, 2, Zarr.BloscCompressor,
            Zarr.DictStore}

        @test length(z.storage.a) === 3
        @test length(z.storage.a["0.0"]) === 64
        @test eltype(z.storage.a["0.0"]) === UInt8
        @test z.metadata.shape[] === (2, 3)
        @test z.metadata.order === 'C'
        @test z.metadata.chunks === (2, 3)
        @test z.metadata.fill_value === nothing
        @test z.metadata.compressor isa Zarr.BloscCompressor
        @test z.metadata.compressor.blocksize === 0
        @test z.metadata.compressor.clevel === 5
        @test z.metadata.compressor.cname === "lz4"
        @test z.metadata.compressor.shuffle === 1
        @test z.attrs == Dict{Any, Any}()
        @test z.writeable === true
        @test_throws ArgumentError zzeros(Int64,2,3, chunks = (0,1))
        @test_throws ArgumentError zzeros(Int64,0,-1)
        @test_throws ArgumentError Zarr.Metadata(zeros(2,2), (2,2), zarr_format = 3)
        @test_throws ArgumentError Zarr.Metadata(zeros(2,2), (2,2), order = 'F')
    end

    @testset "methods" begin
        z = zzeros(Int64, 2, 3)
        @test z isa ZArray{Int64, 2, Zarr.BloscCompressor,
            Zarr.DictStore}

        @test eltype(z) === Int64
        @test ndims(z) === 2
        @test size(z) === (2, 3)
        @test size(z, 2) === 3
        @inferred size(z)
        @inferred size(z, 2)
        @test length(z) === 2 * 3
        @test lastindex(z, 2) === 3
        @test Zarr.zname(z) === "root"
    end

    @testset "NoCompressor DirectoryStore" begin
        mktempdir(@__DIR__) do dir
            name = "nocompressor"
            z = zzeros(Int64, 2, 3, path="$dir/$name",
                compressor=Zarr.NoCompressor())

            @test z.metadata.compressor === Zarr.NoCompressor()
            @test z.storage === Zarr.DirectoryStore("$dir/$name")
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

@testset "Groups" begin
    store = DirectoryStore(tempname())
    g = zgroup(store,"mygroup")
    g2 = zgroup(g,"asubgroup",attrs = Dict("a1"=>5))
    @test Zarr.is_zgroup(store,"mygroup")
    @test Zarr.is_zgroup(store,"mygroup/asubgroup")
    @test g2.attrs["a1"]==5
    @test isdir(joinpath(store.folder,"mygroup"))
    @test isdir(joinpath(store.folder,"mygroup","asubgroup"))
end

@testset "Metadata" begin
    @testset "Data type encoding" begin
        @test Zarr.typestr(Bool) === "<b1"
        @test Zarr.typestr(Int8) === "<i1"
        @test Zarr.typestr(Int64) === "<i8"
        @test Zarr.typestr(UInt32) === "<u4"
        @test Zarr.typestr(UInt128) === "<u16"
        @test Zarr.typestr(Complex{Float32}) === "<c8"
        @test Zarr.typestr(Complex{Float64}) === "<c16"
        @test Zarr.typestr(Float16) === "<f2"
        @test Zarr.typestr(Float64) === "<f8"
        @test Zarr.typestr(Zarr.MaxLengthString{5,UInt8}) === "<S5"
        @test Zarr.typestr(Zarr.MaxLengthString{9,UInt32}) === "<U9"
        @test Zarr.typestr(Vector{Int64}) === "|O"
        @test Zarr.typestr(Zarr.DateTime64{Day}) === "<M8[D]"
        @test Zarr.typestr(Zarr.DateTime64{Nanosecond}) === "<M8[ns]"
    end

    @testset "Metadata struct and JSON representation" begin
        A = fill(1.0, 30, 20)
        chunks = (5,10)
        metadata = Zarr.Metadata(A, chunks; fill_value=-1.5)
        @test metadata isa Zarr.Metadata
        @test metadata.zarr_format === 2
        @test metadata.shape[] === size(A)
        @test metadata.chunks === chunks
        @test metadata.dtype === "<f8"
        @test metadata.compressor === Zarr.BloscCompressor(0, 5, "lz4", true)
        @test metadata.fill_value === -1.5
        @test metadata.order === 'C'
        @test metadata.filters === nothing

        jsonstr = json(metadata)
        metadata_cycled = Zarr.Metadata(jsonstr,false)
        @test metadata == metadata_cycled
    end

    @testset "Fill value" begin
        @test Zarr.fill_value_encoding(Inf) === "Infinity"
        @test Zarr.fill_value_encoding(-Inf) === "-Infinity"
        @test Zarr.fill_value_encoding(NaN) === "NaN"
        @test Zarr.fill_value_encoding(nothing) === nothing
        @test Zarr.fill_value_encoding("-") === "-"

        @test Zarr.fill_value_decoding("Infinity", Float64) === Inf
        @test Zarr.fill_value_decoding("-Infinity", Float64) === -Inf
        @test Zarr.fill_value_decoding("NaN", Float32) === NaN32
        @test Zarr.fill_value_decoding("3.4", Float64) === 3.4
        @test Zarr.fill_value_decoding("3", Int) === 3
        @test Zarr.fill_value_decoding(nothing, Int) === nothing
        @test Zarr.fill_value_decoding("-", String) === "-"
        @test Zarr.fill_value_decoding("", Zarr.ASCIIChar) === nothing
        @test Zarr.fill_value_decoding("", Zarr.MaxLengthString{6,UInt8}) === Zarr.MaxLengthString{6,UInt8}("")
    end
end

@testset "getindex/setindex" begin
  a = zzeros(Int64, 10, 10, chunks = (5,2))
  a[2,:] .= 5
  a[:,3] .= 6
  a[9:10,9:10] .= 2
  a[5,5] = 1

  @test a[2,:] == [5, 5, 6, 5, 5, 5, 5, 5, 5, 5]
  @test a[:,3] == fill(6,10)
  @test a[4,4] == 0
  @test a[5:6,5:6] == [1 0; 0 0]
  @test a[9:10,9:10] == fill(2,2,2)
  # Now with FillValue
  amiss = zzeros(Int64, 10,10,chunks=(5,2), fill_value=-1, fill_as_missing=true)
  amiss[:,1] = 1:10
  amiss[:,2] .= missing
  amiss[1:3,4] = [1,missing,3]
  amiss[1,10] = 5
  amiss[1:5,9:10] .= missing

  @test amiss[:,1] == 1:10
  @test all(ismissing,amiss[:,2])
  @test all(i->isequal(i...),zip(amiss[1:3,4],[1,missing,3]))
  # Test that chunk containing only missings is not initialized
  @test !Zarr.isinitialized(amiss.storage,Zarr.citostring(CartesianIndex((1,5))))
  #
  amiss = zcreate(Int64, 10,10,chunks=(5,2), fill_value=-1, fill_as_missing=false)
  amiss[:,1] = 1:10
  amiss[1:3,4] = [1,-1,3]
  amiss[1,10] = 5
  amiss[1:5,9:10] .= -1

  @test amiss[:,1] == 1:10
  @test all(==(-1),amiss[:,2])
  @test all(i->isequal(i...),zip(amiss[1:3,4],[1,-1,3]))
  # Test that chunk containing only fill values is not initialized
  @test !Zarr.isinitialized(amiss.storage,Zarr.citostring(CartesianIndex((1,5))))
end

@testset "resize" begin
  a = zzeros(Int64, 10, 10, chunks = (5,2), fill_value=-1)
  resize!(a,5,4)
  @test size(a)==(5,4)
  resize!(a,10,10)
  @test size(a)==(10,10)
  @test all(==(-1),a[6:end,:])
  xapp = rand(1:10,10,20)
  append!(a,xapp)
  @test size(a)==(10,30)
  @test a[:,11:30] == xapp
  singlevec = rand(1:10,10)
  append!(a,singlevec)
  @test size(a)==(10,31)
  @test a[:,31]==singlevec
  singlerow = rand(1:10,31)
  append!(a,singlerow,dims=1)
  @test size(a)==(11,31)
  @test a[11,:]==singlerow
  append!(a,vcat(singlerow', singlerow'), dims=1)
  @test size(a)==(13,31)
  @test a[12:13,:]==vcat(singlerow', singlerow')
  @test_throws ArgumentError resize!(a,(-1,2))
end

@testset "string array getindex/setindex" begin
  using Zarr: MaxLengthString
  aa = ["this", "is", "all ", "ascii"]
  bb = ["And" "Unicode"; "ματριξ" missing]
  a = ZArray(aa)
  b = ZArray(bb, fill_value = MaxLengthString{7,UInt32}(""))
  @test eltype(a) == MaxLengthString{5,UInt8}
  @test eltype(b) == Union{MaxLengthString{7,UInt32},Missing}
  @test a[:] == ["this", "is", "all ", "ascii"]
  @test all(isequal.(b[:,:],["And" "Unicode"; "ματριξ" missing]))
end

@testset "ragged arrays" begin
  z = zcreate(Vector{Float64},2,3)
  a, b, c, d = [1.0,2.0,3.0], [4.0,5.0],[2.0],[2.0,3.0]
  z[1,1] = a
  z[2,1:3] = [b,c,d]
  @test z[:,:] == reshape([a,b,[],c,[],d],2,3)
  @test storageratio(z) == "unknown"
  @test zinfo(z) === nothing
  @test z.metadata.filters == (Zarr.VLenArrayFilter{Float64}(),)
  z2 = ZArray(reshape([a,b,Float64[],c,Float64[],d],2,3))
  @test z[:,:] == z2[:,:]
end

@testset "Fillvalue as missing" begin 
    p = tempname()
    a = zcreate(Int,2,3,fill_value=-1,fill_as_missing=true,path=p)
    @test all(ismissing,a[:,:])
    @test eltype(a) == Union{Int,Missing}
    a[:,1] .= 5
    b = zopen(p,fill_as_missing=true)
    @test eltype(b) == Union{Int, Missing}
    @test all(ismissing,b[:,2:3])
    @test all(==(5),b[:,1])
    c = zopen(p)
    @test eltype(c) == Int
    @test all(==(-1),c[:,2:3])
    @test all(==(5),c[:,1])
  end

include("storage.jl")



include("python.jl")

end  # @testset "Zarr"
