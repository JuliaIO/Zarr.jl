# Tests for the statically typed `zopen(::Type{T}, ::Val{N}, ...)` path.
#
# This path parses `.zarray` into the concrete `ZarrayJSON` schema and validates
# the stored document against caller-supplied `T`/`N`/`C`, so the returned
# `ZArray` is fully concretely typed (which is what makes it juliac
# `--trim=safe`). The tests below check the round trips, the validation errors
# and the schema-level helpers.

using Zarr: NoCompressor, ZstdCompressor, ZlibCompressor, BloscCompressor,
    DictStore, DirectoryStore

@testset "Typed open" begin

    @testset "round trip NoCompressor" begin
        p = mktempdir()
        data = reshape(collect(1.0:24.0), 4, 6)
        z0 = zcreate(Float64, DirectoryStore(p), 4, 6; chunks=(2, 3),
                     compressor=NoCompressor(), fill_value=0.0)
        z0[:, :] = data

        z = zopen(Float64, Val(2), DirectoryStore(p), ""; compressor=NoCompressor)
        @test z isa ZArray{Float64,2,DirectoryStore,
                           ZarrCore.MetadataV2{Float64,2,NoCompressor,Nothing}}
        @test eltype(z) === Float64
        @test size(z) == (4, 6)
        @test z[:, :] == data
        @test z[2:3, 4:6] == data[2:3, 4:6]
        @test z[3, 5] == data[3, 5]

        dyn = zopen(p)
        @test z.metadata == dyn.metadata
        @test z.attrs isa Dict{String,Any}
        @test isempty(z.attrs)
        @test z.path == ""
        @test !z.writeable

        # `path::AbstractString` convenience method (wraps a DirectoryStore).
        z2 = zopen(Float64, Val(2), p; compressor=NoCompressor)
        @test z2 isa ZArray{Float64,2,DirectoryStore,
                            ZarrCore.MetadataV2{Float64,2,NoCompressor,Nothing}}
        @test z2[:, :] == data

        # A store-generic path: the same code with a DictStore.
        ds = DictStore()
        zd0 = zcreate(Float64, ds, 3, 2; chunks=(3, 1), compressor=NoCompressor(),
                      fill_value=0.0)
        ddata = [1.0 2.0; 3.0 4.0; 5.0 6.0]
        zd0[:, :] = ddata
        zd = zopen(Float64, Val(2), ds, ""; compressor=NoCompressor)
        @test zd isa ZArray{Float64,2,DictStore,
                            ZarrCore.MetadataV2{Float64,2,NoCompressor,Nothing}}
        @test zd[:, :] == ddata
    end

    @testset "other ranks" begin
        p1 = mktempdir()
        d1 = collect(1:7)
        z1c = zcreate(Int64, DirectoryStore(p1), 7; chunks=(3,),
                      compressor=NoCompressor(), fill_value=0)
        z1c[:] = d1
        z1 = zopen(Int64, Val(1), DirectoryStore(p1), ""; compressor=NoCompressor)
        @test z1 isa ZArray{Int64,1,DirectoryStore,
                            ZarrCore.MetadataV2{Int64,1,NoCompressor,Nothing}}
        @test size(z1) == (7,)
        @test z1[:] == d1

        p3 = mktempdir()
        d3 = reshape(collect(1.0f0:24.0f0), 2, 3, 4)
        z3c = zcreate(Float32, DirectoryStore(p3), 2, 3, 4; chunks=(1, 3, 2),
                      compressor=NoCompressor(), fill_value=0.0f0)
        z3c[:, :, :] = d3
        z3 = zopen(Float32, Val(3), DirectoryStore(p3), ""; compressor=NoCompressor)
        @test z3 isa ZArray{Float32,3,DirectoryStore,
                            ZarrCore.MetadataV2{Float32,3,NoCompressor,Nothing}}
        @test size(z3) == (2, 3, 4)
        @test z3[:, :, :] == d3
        @test z3[:, 2, 3] == d3[:, 2, 3]
    end

    @testset "compressor round trips" begin
        data = reshape(collect(1.0:12.0), 3, 4)

        # Zstd. `ZstdCompressor` wraps a `ZstdEncodeOptions` holding a `Vector`
        # of advanced parameters, so the default struct `==` (which is `===`
        # here) is false for two separately built instances - compare the
        # configuration fields instead.
        pz = mktempdir()
        zz0 = zcreate(Float64, DirectoryStore(pz), 3, 4; chunks=(3, 2),
                      compressor=ZstdCompressor(level=3), fill_value=0.0)
        zz0[:, :] = data
        zz = zopen(Float64, Val(2), DirectoryStore(pz), ""; compressor=ZstdCompressor)
        @test zz isa ZArray{Float64,2,DirectoryStore,
                            ZarrCore.MetadataV2{Float64,2,ZstdCompressor,Nothing}}
        @test zz[:, :] == data
        @test zz.metadata.compressor.config.compressionLevel ==
              zz0.metadata.compressor.config.compressionLevel == 3
        @test zz.metadata.compressor.config.checksum ==
              zz0.metadata.compressor.config.checksum == false

        # Zlib: `ZlibEncodeOptions` is all-isbits, so `==` works directly.
        pl = mktempdir()
        zl0 = zcreate(Float64, DirectoryStore(pl), 3, 4; chunks=(3, 2),
                      compressor=ZlibCompressor(clevel=4), fill_value=0.0)
        zl0[:, :] = data
        zl = zopen(Float64, Val(2), DirectoryStore(pl), ""; compressor=ZlibCompressor)
        @test zl isa ZArray{Float64,2,DirectoryStore,
                            ZarrCore.MetadataV2{Float64,2,ZlibCompressor,Nothing}}
        @test zl[:, :] == data
        @test zl.metadata.compressor == zl0.metadata.compressor == ZlibCompressor(clevel=4)

        # Blosc.
        pb = mktempdir()
        zb0 = zcreate(Float64, DirectoryStore(pb), 3, 4; chunks=(3, 2),
                      compressor=BloscCompressor(clevel=3, cname="lz4", shuffle=1),
                      fill_value=0.0)
        zb0[:, :] = data
        zb = zopen(Float64, Val(2), DirectoryStore(pb), ""; compressor=BloscCompressor)
        @test zb isa ZArray{Float64,2,DirectoryStore,
                            ZarrCore.MetadataV2{Float64,2,BloscCompressor,Nothing}}
        @test zb[:, :] == data
        @test zb.metadata.compressor == zb0.metadata.compressor
        @test zb.metadata.compressor == BloscCompressor(clevel=3, cname="lz4", shuffle=1)

        # The typed metadata agrees with what the dynamic path reads back.
        # (Not for zstd: `ZstdEncodeOptions` holds a `Vector`, so the default
        # struct `==` degenerates to `===` and two separately parsed compressors
        # never compare equal - see the field-wise checks above.)
        @test zl.metadata == zopen(pl).metadata
        @test zb.metadata == zopen(pb).metadata
    end

    @testset "errors" begin
        p = mktempdir()
        z0 = zcreate(Float64, DirectoryStore(p), 4, 6; chunks=(2, 3),
                     compressor=NoCompressor(), fill_value=0.0)

        # dtype mismatch
        @test_throws ArgumentError zopen(Float32, Val(2), DirectoryStore(p), "";
                                         compressor=NoCompressor)
        @test_throws ArgumentError zopen(Int64, Val(2), DirectoryStore(p), "";
                                         compressor=NoCompressor)
        # ndims mismatch
        @test_throws ArgumentError zopen(Float64, Val(1), DirectoryStore(p), "";
                                         compressor=NoCompressor)
        @test_throws ArgumentError zopen(Float64, Val(3), DirectoryStore(p), "";
                                         compressor=NoCompressor)
        # a compressor was requested but the store has none
        @test_throws ArgumentError zopen(Float64, Val(2), DirectoryStore(p), "";
                                         compressor=ZstdCompressor)

        pz = mktempdir()
        zz0 = zcreate(Float64, DirectoryStore(pz), 4, 6; chunks=(2, 3),
                      compressor=ZstdCompressor(level=1), fill_value=0.0)
        # NoCompressor requested on a compressed store
        @test_throws ArgumentError zopen(Float64, Val(2), DirectoryStore(pz), "";
                                         compressor=NoCompressor)
        # wrong compressor type on a compressed store
        @test_throws ArgumentError zopen(Float64, Val(2), DirectoryStore(pz), "";
                                         compressor=ZlibCompressor)
        @test_throws ArgumentError zopen(Float64, Val(2), DirectoryStore(pz), "";
                                         compressor=BloscCompressor)

        # filters are rejected: a String array carries a vlen-utf8 filter.
        pf = mktempdir()
        zf0 = zcreate(String, DirectoryStore(pf), 3; compressor=NoCompressor())
        @test ZarrCore.parse_zarray(read(joinpath(pf, ".zarray"))).filters !== nothing
        @test_throws ArgumentError zopen(String, Val(1), DirectoryStore(pf), "";
                                         compressor=NoCompressor)

        # a hand written `.zarray` with a filter entry
        ph = mktempdir()
        sh = DirectoryStore(ph)
        sh["", ".zarray"] = Vector{UInt8}(codeunits("""
            {"zarr_format":2,"shape":[4],"chunks":[2],"dtype":"<f8",
             "compressor":null,"fill_value":0.0,"order":"C",
             "filters":[{"id":"vlen-utf8"}]}"""))
        @test_throws ArgumentError zopen(Float64, Val(1), DirectoryStore(ph), "";
                                         compressor=NoCompressor)

        # no array at the path
        pe = mktempdir()
        @test_throws ArgumentError zopen(Float64, Val(2), DirectoryStore(pe), "";
                                         compressor=NoCompressor)
        @test_throws ArgumentError zopen(Float64, Val(2), DictStore(), "";
                                         compressor=NoCompressor)

        # leading slash in the path
        @test_throws ArgumentError zopen(Float64, Val(2), DirectoryStore(p), "/sub";
                                         compressor=NoCompressor)
    end

    @testset "fill values and fill_as_missing" begin
        p = mktempdir()
        z0 = zcreate(Float64, DirectoryStore(p), 2, 2; chunks=(1, 2),
                     compressor=NoCompressor(), fill_value=-1.0)
        z0[1, :] = [5.0, 6.0]

        zm = zopen(Float64, Val(2), DirectoryStore(p), ""; compressor=NoCompressor,
                   fill_as_missing=true)
        @test eltype(zm) === Union{Missing,Float64}
        @test zm isa ZArray{Union{Missing,Float64},2,DirectoryStore,
                            ZarrCore.MetadataV2{Union{Missing,Float64},2,NoCompressor,Nothing}}
        am = zm[:, :]
        @test am[1, 1] == 5.0
        @test am[1, 2] == 6.0
        @test am[2, 1] === missing
        @test am[2, 2] === missing

        zn = zopen(Float64, Val(2), DirectoryStore(p), ""; compressor=NoCompressor,
                   fill_as_missing=false)
        @test eltype(zn) === Float64
        @test zn[:, :] == [5.0 6.0; -1.0 -1.0]
        @test zn.metadata.fill_value == -1.0

        # NaN fill value: written as the string "NaN" in `.zarray`.
        pn = mktempdir()
        zn0 = zcreate(Float64, DirectoryStore(pn), 2; compressor=NoCompressor(),
                      fill_value=NaN)
        znn = zopen(Float64, Val(1), DirectoryStore(pn), ""; compressor=NoCompressor)
        @test znn.metadata.fill_value isa Float64
        @test isnan(znn.metadata.fill_value)
        @test all(isnan, znn[:])
        @test ZarrCore.parse_zarray(read(joinpath(pn, ".zarray"))).fill_value.kind ==
              ZarrCore.FV_STRING

        # Integer fill value.
        pi = mktempdir()
        zi0 = zcreate(Int32, DirectoryStore(pi), 2, 2; chunks=(2, 2),
                      compressor=NoCompressor(), fill_value=Int32(7))
        zi = zopen(Int32, Val(2), DirectoryStore(pi), ""; compressor=NoCompressor)
        @test zi isa ZArray{Int32,2,DirectoryStore,
                            ZarrCore.MetadataV2{Int32,2,NoCompressor,Nothing}}
        @test zi.metadata.fill_value === Int32(7)
        @test zi[:, :] == fill(Int32(7), 2, 2)
        @test ZarrCore.parse_zarray(read(joinpath(pi, ".zarray"))).fill_value.kind ==
              ZarrCore.FV_INT

        # Bool fill value.
        pb = mktempdir()
        zb0 = zcreate(Bool, DirectoryStore(pb), 2, 2; chunks=(2, 2),
                      compressor=NoCompressor(), fill_value=false)
        bdata = Bool[true false; false true]
        zb0[:, :] = bdata
        zb = zopen(Bool, Val(2), DirectoryStore(pb), ""; compressor=NoCompressor)
        @test zb.metadata.fill_value === false
        @test zb[:, :] == bdata

        # A null fill value stays null and `fill_as_missing` is then a no-op.
        p0 = mktempdir()
        z00 = zcreate(Float64, DirectoryStore(p0), 2; chunks=(2,),
                      compressor=NoCompressor())
        z00[:] = [1.0, 2.0]
        zz = zopen(Float64, Val(1), DirectoryStore(p0), ""; compressor=NoCompressor,
                   fill_as_missing=true)
        @test eltype(zz) === Float64
        @test zz.metadata.fill_value === nothing
        @test ZarrCore.isnullfill(ZarrCore.parse_zarray(read(joinpath(p0, ".zarray"))).fill_value)
        @test zz[:] == [1.0, 2.0]
    end

    @testset "attrs" begin
        p = mktempdir()
        atts = Dict{String,Any}("units" => "m", "scale" => 2.5, "y" => 1,
                                "nested" => Dict{String,Any}("a" => 1))
        z0 = zcreate(Float64, DirectoryStore(p), 2; chunks=(2,),
                     compressor=NoCompressor(), fill_value=0.0, attrs=atts)
        z = zopen(Float64, Val(1), DirectoryStore(p), ""; compressor=NoCompressor)
        @test z.attrs isa Dict{String,Any}
        @test z.attrs["units"] == "m"
        @test z.attrs["scale"] == 2.5
        @test z.attrs["nested"]["a"] == 1
        # Integers stay integers on the typed path, exactly as on the dynamic
        # one: `getattrs_typed` tries the strict parse first, and only a
        # document with a bare `NaN`/`Infinity` falls back to `allownan=true`
        # (which widens every number to `Float64`).
        @test z.attrs["y"] === 1
        @test zopen(p).attrs["y"] === 1

        # A bare `NaN` literal in `.zattrs` (which older Zarr.jl versions wrote)
        # is invalid JSON; the strict parse fails and the `allownan=true` retry
        # picks it up.
        pn = mktempdir()
        zn0 = zcreate(Float64, DirectoryStore(pn), 2; chunks=(2,),
                      compressor=NoCompressor(), fill_value=0.0)
        sn = DirectoryStore(pn)
        sn["", ".zattrs"] = Vector{UInt8}(codeunits("{\"x\": NaN, \"y\": 1}"))
        zn = zopen(Float64, Val(1), DirectoryStore(pn), ""; compressor=NoCompressor)
        @test isnan(zn.attrs["x"])
        # In *this* document the `allownan=true` retry is what parsed it, so the
        # integer `1` comes back as `1.0`; only the value is checked here.
        @test zn.attrs["y"] == 1
        direct = ZarrCore.getattrs_typed(ZarrCore.ZarrFormat(Val(2)), sn, "")
        @test direct isa Dict{String,Any}
        @test keys(direct) == keys(zn.attrs)
        @test isnan(direct["x"]) && direct["y"] == 1

        # No `.zattrs` at all.
        pe = mktempdir()
        ze0 = zcreate(Float64, DirectoryStore(pe), 2; chunks=(2,),
                      compressor=NoCompressor(), fill_value=0.0)
        rm(joinpath(pe, ".zattrs"); force=true)
        ze = zopen(Float64, Val(1), DirectoryStore(pe), ""; compressor=NoCompressor)
        @test ze.attrs isa Dict{String,Any}
        @test isempty(ze.attrs)
    end

    @testset "zarray schema" begin
        p = mktempdir()
        z0 = zcreate(Float64, DirectoryStore(p), 4, 6; chunks=(2, 3),
                     compressor=ZstdCompressor(level=2), fill_value=0.0)
        zj = ZarrCore.parse_zarray(read(joinpath(p, ".zarray")))
        @test zj isa ZarrCore.ZarrayJSON
        @test zj.zarr_format == 2
        # `.zarray` stores the shape in C order, i.e. reversed w.r.t. Julia.
        @test zj.shape == [6, 4]
        @test zj.chunks == [3, 2]
        @test zj.dtype == "<f8"
        @test zj.order == "C"
        @test zj.compressor isa ZarrCore.CompressorJSON
        @test zj.compressor.id == "zstd"
        @test zj.compressor.level == 2
        @test zj.filters === nothing

        # Parsing from a String works the same way.
        @test ZarrCore.parse_zarray(read(joinpath(p, ".zarray"), String)).dtype == "<f8"

        # Unknown keys are ignored, missing optional keys take their defaults.
        zx = ZarrCore.parse_zarray("""
            {"zarr_format":2,"shape":[3,2],"chunks":[3,2],"dtype":"<i8",
             "compressor":null,"fill_value":null,"order":"C","filters":null,
             "node_type":"array","some_future_key":{"a":[1,2,3]}}""")
        @test zx.shape == [3, 2]
        @test zx.dtype == "<i8"
        @test zx.compressor === nothing
        @test ZarrCore.isnullfill(zx.fill_value)
        @test zx.dimension_separator === nothing

        # A minimal document: everything optional falls back to a default.
        zmin = ZarrCore.parse_zarray("""{"shape":[2],"chunks":[2],"dtype":"<f8"}""")
        @test zmin.zarr_format == 2
        @test zmin.order == "C"
        @test zmin.compressor === nothing
        @test ZarrCore.isnullfill(zmin.fill_value)

        # `fill_value_typed` decodes each on-disk representation.
        @test ZarrCore.fill_value_typed(Float64, ZarrCore.FILL_VALUE_NULL) === nothing
        @test ZarrCore.fill_value_typed(Float64,
            ZarrCore.parse_zarray("""{"fill_value":1.5}""").fill_value) === 1.5
        @test ZarrCore.fill_value_typed(Int32,
            ZarrCore.parse_zarray("""{"fill_value":7}""").fill_value) === Int32(7)
        @test isnan(ZarrCore.fill_value_typed(Float64,
            ZarrCore.parse_zarray("""{"fill_value":"NaN"}""").fill_value))
        @test ZarrCore.fill_value_typed(Float64,
            ZarrCore.parse_zarray("""{"fill_value":"Infinity"}""").fill_value) === Inf
        @test ZarrCore.fill_value_typed(Bool,
            ZarrCore.parse_zarray("""{"fill_value":true}""").fill_value) === true

        # `dimension_separator = '/'` stores nested chunk keys.
        pd = mktempdir()
        data = reshape(collect(1.0:12.0), 3, 4)
        zd0 = zcreate(Float64, DirectoryStore(pd), 3, 4; chunks=(3, 2),
                      compressor=NoCompressor(), fill_value=0.0,
                      dimension_separator='/')
        zd0[:, :] = data
        @test ZarrCore.parse_zarray(read(joinpath(pd, ".zarray"))).dimension_separator == "/"
        zd = zopen(Float64, Val(2), DirectoryStore(pd), ""; compressor=NoCompressor)
        @test zd.metadata.chunk_key_encoding == ZarrCore.ChunkKeyEncoding('/', false)
        @test zd[:, :] == data
        @test zd[:, 3:4] == data[:, 3:4]
    end

    @testset "writing through a typed handle" begin
        p = mktempdir()
        z0 = zcreate(Float64, DirectoryStore(p), 2, 2; chunks=(2, 2),
                     compressor=NoCompressor(), fill_value=0.0)

        zw = zopen(Float64, Val(2), DirectoryStore(p), ""; compressor=NoCompressor,
                   mode="w")
        @test zw.writeable
        zw[1, 1] = 42.0
        zw[2, 2] = -3.0
        @test zopen(p)[:, :] == [42.0 0.0; 0.0 -3.0]
        @test zopen(Float64, Val(2), DirectoryStore(p), ""; compressor=NoCompressor)[1, 1] == 42.0

        zr = zopen(Float64, Val(2), DirectoryStore(p), ""; compressor=NoCompressor)
        @test !zr.writeable
        @test_throws ErrorException zr[1, 1] = 1.0

        # ... and through the `path` convenience method.
        zw2 = zopen(Float64, Val(2), p; compressor=NoCompressor, mode="w")
        zw2[1, 2] = 7.0
        @test zopen(p)[1, 2] == 7.0
    end

end
