using Test, JSON, ZarrTrimmable

# `ZarrCore` and the three codec packages are direct dependencies of this test
# target when the file runs standalone (`Pkg.test("ZarrTrimmable")`), and are
# reachable only through the `Zarr` facade when it runs as part of the Zarr.jl
# test suite (`test/trimmable.jl`). Both branches put the same names in scope.
@static if Base.identify_package("ZarrCore") === nothing
    using Zarr
    using Zarr: ZarrCore, ZarrZstd, ZarrZlib, ZarrBlosc
else
    using ZarrCore, ZarrZstd, ZarrZlib, ZarrBlosc
end

using ZarrTrimmable: DFloat64, DFloat32, DInt32, DInt64, DUInt8,
                     NoComp, Zstd, Zlib, Blosc,
                     FillNone, FillNumber, FillNaN, FillInt, ArrayMeta

# `Base.infer_return_type` is 1.11+; `Base.return_types` covers the 1.10 LTS.
_rt(f, T) = VERSION >= v"1.11" ? Base.infer_return_type(f, T) : only(Base.return_types(f, T))

# Local visitors (the spike shipped these; the package deliberately does not).
function sum_all(z::ZArray{T,2}) where {T}
    a = z[:, :]
    s = 0.0
    for x in a
        s += Float64(x)
    end
    return s
end

function sum_all(z::ZArray{T,3}) where {T}
    a = z[:, :, :]
    s = 0.0
    for x in a
        s += Float64(x)
    end
    return s
end

function fill_iota!(z::ZArray{T,2}) where {T}
    n1 = size(z, 1); n2 = size(z, 2)
    a = Array{T,2}(undef, n1, n2)
    v = one(T)
    for j in 1:n2, i in 1:n1
        a[i, j] = v
        v += one(T)
    end
    z[:, :] = a
    return nothing
end

function fill_iota!(z::ZArray{T,3}) where {T}
    n1 = size(z, 1); n2 = size(z, 2); n3 = size(z, 3)
    a = Array{T,3}(undef, n1, n2, n3)
    v = one(T)
    for k in 1:n3, j in 1:n2, i in 1:n1
        a[i, j, k] = v
        v += one(T)
    end
    z[:, :, :] = a
    return nothing
end

# ---------------------------------------------------------------------------
# fixtures: the 16 small v2 DirectoryStores the spike kept in `data/`
# {Float64,Int32} x {(4,6)/(2,3), (4,6,2)/(2,3,1)} x {none,zstd,zlib,blosc},
# filled with 1..n column-major.  2-d sums to 300.0, 3-d to 1176.0.
# ---------------------------------------------------------------------------

const TRIM_DATADIR = mktempdir()

# (spec, on-disk name, live compressor) for the four compressors
const TRIM_COMPS = ((NoComp(), "none", ZarrCore.NoCompressor()),
               (Zstd(3), "zstd", ZarrZstd.ZstdCompressor(level = 3)),
               (Zlib(6), "zlib", ZarrZlib.ZlibCompressor(clevel = 6)),
               (Blosc("lz4", 5, 1, 0), "blosc", ZarrBlosc.BloscCompressor()))
# (dtype, name, julia type, fill value)
const TRIM_DTS = ((DFloat64(), "f64", Float64, FillNumber(0.0)),
             (DInt32(), "i32", Int32, FillInt(0)))
# (shape, chunks, name) -- Julia (fastest-first) order
const TRIM_SHAPES = (([4, 6], [2, 3], "2d"), ([4, 6, 2], [2, 3, 1], "3d"))

function _gen_fixtures!(dir)
    made = String[]
    for (_, tn, T, _) in TRIM_DTS, (shape, chunks, dn) in TRIM_SHAPES, (_, cn, comp) in TRIM_COMPS
        name = string(tn, "_", dn, "_", cn, ".zarr")
        path = joinpath(dir, name)
        isdir(path) && rm(path; recursive = true)
        dims = Tuple(shape)
        z = ZarrCore.zcreate(T, DirectoryStore(path), dims...;
                             chunks = Tuple(chunks),
                             compressor = comp,
                             fill_value = zero(T))
        n = prod(dims)
        data = reshape(T[T(i) for i in 1:n], dims)   # column-major 1..n
        if length(dims) == 2
            z[:, :] = data
        else
            z[:, :, :] = data
        end
        push!(made, name)
    end
    return made
end

const TRIM_FIXTURES = _gen_fixtures!(TRIM_DATADIR)

# The same 16 arrays again as Zarr v3 stores (`<name>_v3.zarr`, `zarr.json`).
# `zcreate(...; zarr_format = 3)` maps each v2 compressor onto its v3 codec
# (`ZstdCompressor` -> `zstd`, `ZlibCompressor` -> `gzip`, `BloscCompressor` ->
# `blosc`, `NoCompressor` -> no bytes->bytes codec at all).
function _gen_fixtures_v3!(dir)
    made = String[]
    for (_, tn, T, _) in TRIM_DTS, (shape, chunks, dn) in TRIM_SHAPES, (_, cn, comp) in TRIM_COMPS
        name = string(tn, "_", dn, "_", cn, "_v3.zarr")
        path = joinpath(dir, name)
        isdir(path) && rm(path; recursive = true)
        dims = Tuple(shape)
        z = ZarrCore.zcreate(T, DirectoryStore(path), dims...;
                             chunks = Tuple(chunks),
                             zarr_format = 3,
                             compressor = comp,
                             fill_value = zero(T))
        n = prod(dims)
        data = reshape(T[T(i) for i in 1:n], dims)   # column-major 1..n
        if length(dims) == 2
            z[:, :] = data
        else
            z[:, :, :] = data
        end
        push!(made, name)
    end
    return made
end

const TRIM_FIXTURES_V3 = _gen_fixtures_v3!(TRIM_DATADIR)

# Two v3 stores `zcreate` cannot express, written through a hand-built
# `MetadataV3`: a three-codec chain `[bytes, zstd, crc32c]` (which the pool has
# to accept, because its bytes->bytes stage is a `Vector`) and a chain with a
# leading `transpose` (which it has to reject, because its array->array stage is
# `Tuple{}`).
function _write_handbuilt_v3(path, md, data)
    store = DirectoryStore(path)
    z = ZArray(md, store, "", Dict{String,Any}(), true)
    ZarrCore.writemetadata(ZarrCore.zarr_format(md), store, "", md)
    z[(Colon() for _ in 1:ndims(data))...] = data
    return z
end

const TRIM_CRC_PATH = joinpath(TRIM_DATADIR, "f64_2d_zstdcrc_v3.zarr")
const TRIM_TRANSPOSE_PATH = joinpath(TRIM_DATADIR, "f64_2d_transpose_v3.zarr")

let data = reshape(Float64[i for i in 1:24], 4, 6)
    p1 = ZarrCore.V3Pipeline((), ZarrCore.BytesCodec(:little),
                             (ZarrZstd.ZstdV3Codec(3), ZarrCore.CRC32cV3Codec()))
    _write_handbuilt_v3(TRIM_CRC_PATH,
        ZarrCore.MetadataV3{Float64,2,typeof(p1)}(
            3, "array", (4, 6), (2, 3), ZarrCore.typestr3(Float64), p1, 0.0,
            ZarrCore.ChunkKeyEncoding('/', true)),
        data)
    p2 = ZarrCore.V3Pipeline((ZarrCore.TransposeCodec((2, 1)),),
                             ZarrCore.BytesCodec(:little), ())
    _write_handbuilt_v3(TRIM_TRANSPOSE_PATH,
        ZarrCore.MetadataV3{Float64,2,typeof(p2)}(
            3, "array", (4, 6), (2, 3), ZarrCore.typestr3(Float64), p2, 0.0,
            ZarrCore.ChunkKeyEncoding('/', true)),
        data)
end

@testset "ZarrTrimmable fixtures" begin
    @test length(TRIM_FIXTURES) == 16
    @test all(n -> isfile(joinpath(TRIM_DATADIR, n, ".zarray")), TRIM_FIXTURES)
    # the dynamic zopen agrees with the intended contents
    for name in TRIM_FIXTURES
        z = zopen(joinpath(TRIM_DATADIR, name))
        a = Array(z)
        n = length(a)
        @test sum(Int.(a)) == n * (n + 1) ÷ 2
    end

    @test length(TRIM_FIXTURES_V3) == 16
    @test all(n -> isfile(joinpath(TRIM_DATADIR, n, "zarr.json")), TRIM_FIXTURES_V3)
    for name in TRIM_FIXTURES_V3
        z = zopen(joinpath(TRIM_DATADIR, name))
        a = Array(z)
        n = length(a)
        @test sum(Int.(a)) == n * (n + 1) ÷ 2
    end
    # the two hand-built stores
    for p in (TRIM_CRC_PATH, TRIM_TRANSPOSE_PATH)
        @test isfile(joinpath(p, "zarr.json"))
        @test sum(Int.(Array(zopen(p)))) == 300
    end
    @test [c["name"] for c in JSON.parse(read(joinpath(TRIM_CRC_PATH, "zarr.json"), String))["codecs"]] ==
          ["bytes", "zstd", "crc32c"]
    @test [c["name"] for c in JSON.parse(read(joinpath(TRIM_TRANSPOSE_PATH, "zarr.json"), String))["codecs"]] ==
          ["transpose", "bytes"]
end

# ---------------------------------------------------------------------------
# ArrayMeta round trips
# ---------------------------------------------------------------------------

const TRIM_J = "{\"zarr_format\":2,\"node_type\":\"array\",\"shape\":[6,4],\"chunks\":[3,2]," *
          "\"dtype\":\"<f8\",\"compressor\":{\"id\":\"blosc\",\"cname\":\"lz4\",\"clevel\":5," *
          "\"shuffle\":1,\"blocksize\":0},\"fill_value\":0.0,\"order\":\"C\"," *
          "\"filters\":null,\"dimension_separator\":\".\"}"

const TRIM_JBLOSC = "{\"id\":\"blosc\",\"cname\":\"lz4\",\"clevel\":5,\"shuffle\":1,\"blocksize\":0}"

@testset "ArrayMeta" begin
    m = ZarrTrimmable.read_meta(TRIM_J)
    @test m isa ArrayMeta
    @test m.dtype isa DFloat64
    @test ZarrTrimmable.julia_type(m.dtype) === Float64
    @test ZarrTrimmable.typestr(m.dtype) == "<f8"
    @test m.shape == [4, 6]          # Julia order, reversed from disk
    @test m.chunks == [2, 3]
    @test ndims(m) == 2
    @test m.compressor isa Blosc
    @test m.compressor.cname == "lz4"
    @test m.compressor.clevel == 5
    @test ZarrTrimmable.compressor_id(m.compressor) == "blosc"
    @test m.fill_value isa FillNumber
    @test m.fill_value.value === 0.0
    @test m.order == 'C'
    @test m.dimension_separator == '.'
    @test ZarrTrimmable.write_meta_string(m) == TRIM_J
    @test occursin("blosc", ZarrTrimmable.describe(m))
    @test occursin("Float64 4x6 chunks=2x3 ndims=2", ZarrTrimmable.describe(m))

    # `write_meta(io, m)` and `write_meta_string(m)` agree
    io = IOBuffer()
    ZarrTrimmable.write_meta(io, m)
    @test String(take!(io)) == TRIM_J

    # `read_meta` also takes bytes
    @test ZarrTrimmable.write_meta_string(ZarrTrimmable.read_meta(Vector{UInt8}(codeunits(TRIM_J)))) == TRIM_J

    @testset "dtype set" begin
        for (s, D, T) in (("<f8", DFloat64, Float64), ("<f4", DFloat32, Float32),
                          ("<i4", DInt32, Int32), ("<i8", DInt64, Int64),
                          ("|u1", DUInt8, UInt8), ("<u1", DUInt8, UInt8))
            d = ZarrTrimmable.dtype_from_typestr(s)
            @test d isa D
            @test ZarrTrimmable.julia_type(d) === T
        end
        # `typestr` is the inverse for the canonical spellings
        for s in ("<f8", "<f4", "<i4", "<i8")
            @test ZarrTrimmable.typestr(ZarrTrimmable.dtype_from_typestr(s)) == s
        end
        @test_throws ArgumentError ZarrTrimmable.dtype_from_typestr("<c16")
        @test_throws ArgumentError ZarrTrimmable.dtype_from_typestr("|S8")
    end

    @testset "fill value variants" begin
        for (txt, T) in (("null", FillNone), ("\"NaN\"", FillNaN),
                         ("\"Infinity\"", FillNumber), ("3", FillInt), ("2.5", FillNumber))
            j = replace(TRIM_J, "\"fill_value\":0.0" => string("\"fill_value\":", txt))
            mm = ZarrTrimmable.read_meta(j)
            @test mm.fill_value isa T
            @test ZarrTrimmable.write_meta_string(mm) == j
        end
        j = replace(TRIM_J, "\"fill_value\":0.0" => "\"fill_value\":\"-Infinity\"")
        mm = ZarrTrimmable.read_meta(j)
        @test mm.fill_value isa FillNumber
        @test mm.fill_value.value == -Inf
        @test ZarrTrimmable.write_meta_string(mm) == j
        # booleans map onto FillInt
        jb = replace(TRIM_J, "\"fill_value\":0.0" => "\"fill_value\":true")
        @test ZarrTrimmable.read_meta(jb).fill_value === FillInt(1)
        jf = replace(TRIM_J, "\"fill_value\":0.0" => "\"fill_value\":false")
        @test ZarrTrimmable.read_meta(jf).fill_value === FillInt(0)
        # an unsupported string spelling
        jx = replace(TRIM_J, "\"fill_value\":0.0" => "\"fill_value\":\"nope\"")
        @test_throws ArgumentError ZarrTrimmable.read_meta(jx)
    end

    @testset "compressor variants" begin
        for (id, T) in (("zstd", Zstd), ("zlib", Zlib))
            j = replace(TRIM_J, TRIM_JBLOSC => string("{\"id\":\"", id, "\",\"level\":3}"))
            mm = ZarrTrimmable.read_meta(j)
            @test mm.compressor isa T
            @test mm.compressor.level == 3
            @test ZarrTrimmable.compressor_id(mm.compressor) == id
            @test ZarrTrimmable.write_meta_string(mm) == j
        end
        j = replace(TRIM_J, TRIM_JBLOSC => "null")
        @test ZarrTrimmable.read_meta(j).compressor isa NoComp
        @test ZarrTrimmable.compressor_id(NoComp()) == ""
        @test ZarrTrimmable.write_meta_string(ZarrTrimmable.read_meta(j)) == j
        # `gzip` is an alias for `zlib`, and `clevel` stands in for `level`
        jg = replace(TRIM_J, TRIM_JBLOSC => "{\"id\":\"gzip\",\"clevel\":4}")
        @test ZarrTrimmable.read_meta(jg).compressor === Zlib(4)
        # defaults for absent keys
        @test ZarrTrimmable.compressor_from_json(ZarrCore.CompressorJSON("zstd")) === Zstd(0)
        @test ZarrTrimmable.compressor_from_json(ZarrCore.CompressorJSON("zlib")) === Zlib(-1)
        @test ZarrTrimmable.compressor_from_json(ZarrCore.CompressorJSON("blosc")) ===
              Blosc("lz4", 5, 1, 0)
        @test ZarrTrimmable.compressor_from_json(nothing) === NoComp()
        @test_throws ArgumentError ZarrTrimmable.compressor_from_json(
            ZarrCore.CompressorJSON("lzma"))
    end

    @testset "rejected documents" begin
        @test_throws ArgumentError ZarrTrimmable.read_meta(replace(TRIM_J, "\"zarr_format\":2" => "\"zarr_format\":3"))
        @test_throws ArgumentError ZarrTrimmable.read_meta(replace(TRIM_J, "\"chunks\":[3,2]" => "\"chunks\":[3,2,1]"))
        @test_throws ArgumentError ZarrTrimmable.read_meta(replace(TRIM_J, "\"filters\":null" => "\"filters\":[{\"id\":\"delta\"}]"))
    end

    @testset "read_meta_path and the store fixtures" begin
        for (_, tn, _, _) in TRIM_DTS, (shape, chunks, dn) in TRIM_SHAPES, (spec, cn, _) in TRIM_COMPS
            path = joinpath(TRIM_DATADIR, string(tn, "_", dn, "_", cn, ".zarr"))
            mm = ZarrTrimmable.read_meta_path(path)
            @test mm.shape == shape
            @test mm.chunks == chunks
            @test mm.compressor isa typeof(spec)
            @test ndims(mm) == length(shape)
            # the `.zarray` ZarrCore wrote round-trips byte-identically
            @test ZarrTrimmable.write_meta_string(mm) ==
                  read(joinpath(path, ".zarray"), String)
            # ... and `read_meta` on the directory / on the file agree
            @test ZarrTrimmable.write_meta_string(ZarrTrimmable.read_meta(path)) ==
                  ZarrTrimmable.write_meta_string(mm)
            @test ZarrTrimmable.write_meta_string(
                      ZarrTrimmable.read_meta_path(joinpath(path, ".zarray"))) ==
                  ZarrTrimmable.write_meta_string(mm)
        end
    end

    @testset "inference" begin
        @test _rt(ZarrTrimmable.read_meta, Tuple{String}) === ArrayMeta
        @test _rt(ZarrTrimmable.read_meta, Tuple{Vector{UInt8}}) === ArrayMeta
        @test _rt(ZarrTrimmable.dtype_from_typestr, Tuple{String}) ===
              ZarrTrimmable.DType
        @test _rt(ZarrTrimmable.compressor_from_json,
                  Tuple{Union{Nothing,ZarrCore.CompressorJSON}}) ===
              ZarrTrimmable.CompressorSpec
        @test _rt(ZarrTrimmable.fill_from_json, Tuple{ZarrCore.FillValueJSON}) ===
              ZarrTrimmable.FillValue
        @test _rt(ZarrTrimmable.write_meta_string, Tuple{ArrayMeta}) === String
        @test _rt(ZarrTrimmable.describe, Tuple{ArrayMeta}) === String
    end
end

@testset "ZarrCore.codec_id" begin
    @test ZarrCore.codec_id(ZarrCore.NoCompressor) == ""
    @test ZarrCore.codec_id(ZarrZstd.ZstdCompressor) == "zstd"
    @test ZarrCore.codec_id(ZarrZlib.ZlibCompressor) == "zlib"
    @test ZarrCore.codec_id(ZarrBlosc.BloscCompressor) == "blosc"
    # `codec_id` (type -> id) and `compressor_id` (spec -> id) agree
    @test ZarrCore.codec_id(ZarrCore.NoCompressor) == ZarrTrimmable.compressor_id(NoComp())
    @test ZarrCore.codec_id(ZarrZstd.ZstdCompressor) == ZarrTrimmable.compressor_id(Zstd(3))
    @test ZarrCore.codec_id(ZarrZlib.ZlibCompressor) == ZarrTrimmable.compressor_id(Zlib(6))
    @test ZarrCore.codec_id(ZarrBlosc.BloscCompressor) ==
          ZarrTrimmable.compressor_id(Blosc("lz4", 5, 1, 0))
    @test _rt(ZarrCore.codec_id, Tuple{Type{ZarrZstd.ZstdCompressor}}) === String
end

# ---------------------------------------------------------------------------
# @zarr_reader pools
#
# `@zarr_reader` defines structs and consts, so it is a TOP-LEVEL macro: it
# cannot be expanded inside a `@testset`/function body ("unsupported const
# declaration on local variable").
# ---------------------------------------------------------------------------

# wide reader: 2 dtypes x 2 ranks x 4 codecs x 1 store
const TRIM_PR = @zarr_reader(dtypes = (Float64, Int32), ndims = (2, 3),
                        codecs = (ZarrCore.NoCompressor, ZarrZstd.ZstdCompressor,
                                  ZarrZlib.ZlibCompressor, ZarrBlosc.BloscCompressor),
                        stores = (DirectoryStore,), tag = T4)
# narrow reader (2 dtype x ndims variants <= 3): the returning `zopen` exists
const TRIM_PN = @zarr_reader(dtypes = (Float64, Int32), ndims = (2,),
                        codecs = (ZarrCore.NoCompressor, ZarrZstd.ZstdCompressor,
                                  ZarrZlib.ZlibCompressor, ZarrBlosc.BloscCompressor),
                        stores = (DirectoryStore, ZarrCore.DictStore), tag = T2)
const TRIM_PE = @zarr_reader(dtypes = (Float64,), ndims = (2,),
                        codecs = (ZarrCore.NoCompressor, ZarrZstd.ZstdCompressor),
                        stores = (DirectoryStore,), tag = TE)
const TRIM_PZ = @zarr_reader(dtypes = (Float64,), ndims = (2,), codecs = (ZarrZstd.ZstdCompressor,),
                        stores = (DirectoryStore,), tag = TZ)
# `read_strategy = forward` forwards the members' real strategy
const TRIM_PF = @zarr_reader(dtypes = (Float64,), ndims = (2,),
                        codecs = (ZarrCore.NoCompressor, ZarrZstd.ZstdCompressor),
                        stores = (DirectoryStore, ZarrCore.DictStore), tag = TF,
                        read_strategy = forward)

# --- Zarr v3 readers -------------------------------------------------------
# both formats, wide: 2 dtypes x 2 ranks x 2 formats = 8 union members, so the
# returning `zopen` is refused
const TRIM_PV23 = @zarr_reader(dtypes = (Float64, Int32), ndims = (2, 3),
                        codecs = (ZarrCore.NoCompressor, ZarrZstd.ZstdCompressor,
                                  ZarrZlib.ZlibCompressor, ZarrBlosc.BloscCompressor),
                        stores = (DirectoryStore,),
                        zarr_format = (2, 3),
                        v3_codecs = (ZarrZstd.ZstdV3Codec, ZarrZlib.GzipV3Codec,
                                     ZarrBlosc.BloscV3Codec, ZarrCore.CRC32cV3Codec),
                        tag = V23)
# both formats, narrow: 1 dtype x 1 rank x 2 formats = 2 union members
const TRIM_PV3N = @zarr_reader(dtypes = (Float64,), ndims = (2,),
                        codecs = (ZarrCore.NoCompressor, ZarrZstd.ZstdCompressor),
                        stores = (DirectoryStore,),
                        zarr_format = (2, 3), v3_codecs = (ZarrZstd.ZstdV3Codec,),
                        tag = V3N)
# v3 only, and with no `codecs` pool at all: no `CodecPool_V3O`, no `zcreate`
const TRIM_PV3 = @zarr_reader(dtypes = (Float64,), ndims = (2,),
                        stores = (DirectoryStore,),
                        zarr_format = 3, v3_codecs = (ZarrZstd.ZstdV3Codec,),
                        tag = V3O)
# v3 only, blosc only -- used for the "codec outside the pool" error
const TRIM_PV3B = @zarr_reader(dtypes = (Float64,), ndims = (2,),
                        stores = (DirectoryStore,),
                        zarr_format = (3,), v3_codecs = (ZarrBlosc.BloscV3Codec,),
                        tag = V3B)

@testset "@zarr_reader pools" begin
    @testset "visitor zopen over all 16 stores" begin
        for (dn, T) in (("f64", Float64), ("i32", Int32))
            for (sn, N, want) in (("2d", 2, 300.0), ("3d", 3, 1176.0))
                for cn in ("none", "zstd", "zlib", "blosc")
                    path = joinpath(TRIM_DATADIR, string(dn, "_", sn, "_", cn, ".zarr"))
                    @test zopen(sum_all, TRIM_PR, path) == want
                    @test zopen(z -> eltype(z), TRIM_PR, path) === T
                    @test zopen(z -> ndims(z), TRIM_PR, path) == N
                    @test zopen(z -> isconcretetype(typeof(z)), TRIM_PR, path)
                    # the wrapper types really are the ones on the array
                    @test zopen(z -> typeof(z.storage), TRIM_PR, path) === StorePool_T4
                    @test zopen(z -> typeof(z.metadata.compressor), TRIM_PR, path) === CodecPool_T4
                    # the explicit-store form
                    sp = StorePool_T4(DirectoryStore(path))
                    @test zopen(sum_all, TRIM_PR, sp, "") == want
                end
            end
        end
        # closures capturing outer state are fine as visitors
        acc = Ref(0.0)
        zopen(z -> (acc[] = sum_all(z); nothing), TRIM_PR, joinpath(TRIM_DATADIR, "f64_2d_zstd.zarr"))
        @test acc[] == 300.0
    end

    @testset "returning zopen (<= 3 variants) and the wide-reader refusal" begin
        p = joinpath(TRIM_DATADIR, "f64_2d_zstd.zarr")
        z = zopen(TRIM_PN, p)::ZArrayUnion_T2
        @test z isa ZArray{Float64,2,StorePool_T2,ZarrCore.MetadataV2{Float64,2,CodecPool_T2,Nothing}}
        @test sum_all(z) == 300.0
        @test size(z) == (4, 6)
        zi = zopen(TRIM_PN, joinpath(TRIM_DATADIR, "i32_2d_blosc.zarr"))::ZArrayUnion_T2
        @test eltype(zi) === Int32
        @test sum_all(zi) == 300.0
        # the 4-variant reader refuses the returning form
        @test_throws ArgumentError zopen(TRIM_PR, p)
        @test_throws ArgumentError zopen(TRIM_PR, StorePool_T4(DirectoryStore(p)), "")
    end

    @testset "store pool: ZarrCore.DictStore member" begin
        src = joinpath(TRIM_DATADIR, "f64_2d_zstd.zarr")
        d = ZarrCore.DictStore()
        for k in readdir(src)
            isfile(joinpath(src, k)) && (d[k] = read(joinpath(src, k)))
        end
        sp = StorePool_T2(d)
        @test zopen(sum_all, TRIM_PN, sp, "") == 300.0
        @test (zopen(TRIM_PN, sp, "")::ZArrayUnion_T2) isa ZArray
        @test sp[".zarray"] isa Vector{UInt8}
        @test ZarrCore.isinitialized(sp, ".zarray")
        # setindex!/delete! forward too
        sp["extra"] = UInt8[1, 2, 3]
        @test sp["extra"] == UInt8[1, 2, 3]
        delete!(sp, "extra")
        @test sp["extra"] === nothing
    end

    @testset "zcreate round trip" begin
        tmp = mktempdir()
        try
            for (T, dims, chunks, want) in ((Float64, (4, 6), (2, 3), 300.0),
                                            (Int32, (4, 6, 2), (2, 3, 1), 1176.0))
                for comp in (ZarrCore.NoCompressor(), ZarrZstd.ZstdCompressor(; level = 3),
                             ZarrZlib.ZlibCompressor(6), ZarrBlosc.BloscCompressor())
                    path = joinpath(tmp, string(T, "_", length(dims), "_",
                                                ZarrCore.codec_id(typeof(comp)), "x"))
                    z = zcreate(TRIM_PR, path, T, dims; codec = comp, chunks = chunks)
                    @test z isa ZArray{T,length(dims),StorePool_T4,
                                       ZarrCore.MetadataV2{T,length(dims),CodecPool_T4,Nothing}}
                    fill_iota!(z)
                    @test zopen(sum_all, TRIM_PR, path) == want
                    # the `.zarray` the pool wrote is what the bare codec writes
                    doc = JSON.parse(read(joinpath(path, ".zarray"), String))
                    @test doc["compressor"] == JSON.parse(ZarrTrimmable._json_string(JSON.lower(comp)))
                    @test doc["dtype"] == ZarrCore.typestr(T)
                    @test doc["chunks"] == reverse(collect(chunks))
                    # ... and ZarrTrimmable's own parser agrees with it
                    mm = ZarrTrimmable.read_meta_path(path)
                    @test collect(mm.shape) == collect(dims)
                    @test ZarrTrimmable.compressor_id(mm.compressor) ==
                          ZarrCore.codec_id(typeof(comp))
                end
            end
            # fill_value round trip
            pf = joinpath(tmp, "fill.zarr")
            zf = zcreate(TRIM_PR, pf, Float64, (4, 6); chunks = (2, 3), fill_value = 0.0)
            @test zf.metadata.fill_value === 0.0
            fill_iota!(zf)
            @test zopen(sum_all, TRIM_PR, pf) == 300.0
            # the explicit-store form and a sub-path
            ps = joinpath(tmp, "grp")
            zs = zcreate(TRIM_PR, StorePool_T4(DirectoryStore(ps)), Float64, (4, 6);
                         chunks = (2, 3), sub = "inner")
            fill_iota!(zs)
            @test zopen(sum_all, TRIM_PR, StorePool_T4(DirectoryStore(ps)), "inner") == 300.0
            # the default codec is the first member of the pool (ZarrCore.NoCompressor)
            pd = joinpath(tmp, "default.zarr")
            zd = zcreate(TRIM_PR, pd, Float64, (4, 6); chunks = (2, 3))
            @test zd.metadata.compressor.c isa ZarrCore.NoCompressor
            @test JSON.parse(read(joinpath(pd, ".zarray"), String))["compressor"] === nothing
        finally
            rm(tmp; recursive = true, force = true)
        end
    end

    @testset "pool forwarding methods in isolation" begin
        cp = CodecPool_T4(ZarrZstd.ZstdCompressor(; level = 5))
        @test cp isa ZarrCore.Compressor
        @test JSON.lower(cp) isa JSON.JSONText
        @test ZarrTrimmable._json_string(JSON.lower(cp)) ==
              ZarrTrimmable._json_string(JSON.lower(ZarrZstd.ZstdCompressor(; level = 5)))
        @test ZarrTrimmable._json_string(JSON.lower(CodecPool_T4(ZarrCore.NoCompressor()))) == "null"
        # every member survives a compress/uncompress round trip through the pool
        for m in (ZarrCore.NoCompressor(), ZarrZstd.ZstdCompressor(; level = 3), ZarrZlib.ZlibCompressor(6),
                  ZarrBlosc.BloscCompressor())
            a = collect(1.0:24.0)
            buf = UInt8[]
            ZarrCore.zcompress!(buf, a, CodecPool_T4(m))
            out = zeros(Float64, 24)
            ZarrCore.zuncompress!(out, buf, CodecPool_T4(m))
            @test out == a
        end
        # getCompressor picks the right member out of the pool
        for (id, M) in (("zstd", ZarrZstd.ZstdCompressor), ("zlib", ZarrZlib.ZlibCompressor),
                        ("blosc", ZarrBlosc.BloscCompressor))
            j = ZarrCore.CompressorJSON(id; cname = "lz4", clevel = 5, shuffle = 1,
                                        blocksize = 0, level = 6, checksum = false)
            @test ZarrCore.getCompressor(CodecPool_T4, j).c isa M
        end
        @test ZarrCore.getCompressor(CodecPool_T4, nothing).c isa ZarrCore.NoCompressor
        # store pool forwards
        sp = StorePool_T2(DirectoryStore(joinpath(TRIM_DATADIR, "f64_2d_zstd.zarr")))
        @test sp[".zarray"] isa Vector{UInt8}
        @test sp["nope"] === nothing
        @test ZarrCore.isinitialized(sp, ".zarray")
        @test !ZarrCore.isinitialized(sp, "nope")
        @test ZarrCore.storagesize(sp, "") > 0
        @test ".zarray" in ZarrCore.subkeys(sp, "")
        @test ZarrCore.subdirs(sp, "") isa AbstractVector
        @test ZarrCore.store_read_strategy(sp) === ZarrCore.SequentialRead()
        @test ZarrCore.has_configurable_missing_chunks(sp) ===
              ZarrCore.has_configurable_missing_chunks(DirectoryStore(joinpath(TRIM_DATADIR, "f64_2d_zstd.zarr")))
        @test sprint(show, sp) == "StorePool_T2"
        @test sp isa ZarrCore.AbstractStore
    end

    @testset "read_strategy = forward" begin
        # pinned by default ...
        @test ZarrCore.store_read_strategy(StorePool_T2(ZarrCore.DictStore())) ===
              ZarrCore.SequentialRead()
        # ... forwarded when asked for
        for s in (DirectoryStore(joinpath(TRIM_DATADIR, "f64_2d_zstd.zarr")), ZarrCore.DictStore())
            @test ZarrCore.store_read_strategy(StorePool_TF(s)) ===
                  ZarrCore.store_read_strategy(s)
        end
        @test zopen(sum_all, TRIM_PF, joinpath(TRIM_DATADIR, "f64_2d_zstd.zarr")) == 300.0
    end

    @testset "errors" begin
        # compressor id not in the pool
        @test_throws ArgumentError zopen(sum_all, TRIM_PE,
                                         joinpath(TRIM_DATADIR, "f64_2d_blosc.zarr"))
        @test_throws ArgumentError ZarrCore.getCompressor(CodecPool_TE,
            ZarrCore.CompressorJSON("blosc"; cname = "lz4", clevel = 5, shuffle = 1,
                                    blocksize = 0))
        # a pool without ZarrCore.NoCompressor cannot accept an uncompressed store
        @test_throws ArgumentError zopen(sum_all, TRIM_PZ,
                                         joinpath(TRIM_DATADIR, "f64_2d_none.zarr"))
        @test_throws ArgumentError ZarrCore.getCompressor(CodecPool_TZ, nothing)
        # dtype outside the pool
        @test_throws ArgumentError zopen(sum_all, TRIM_PE,
                                         joinpath(TRIM_DATADIR, "i32_2d_none.zarr"))
        # rank outside the pool
        @test_throws ArgumentError zopen(sum_all, TRIM_PE,
                                         joinpath(TRIM_DATADIR, "f64_3d_none.zarr"))
        # no array at the path
        @test_throws ArgumentError zopen(sum_all, TRIM_PE, TRIM_DATADIR)
    end

    @testset "macro argument errors" begin
        mkreader(s) = eval(Meta.parse(s))
        # `macroexpand` wraps a macro's `error(...)` in a `LoadError` on some
        # Julia versions and rethrows it bare on others; accept either and
        # assert on the message.
        function macro_error_msg(s::String)
            try
                mkreader(s)
            catch e
                e isa LoadError && (e = e.error)
                return sprint(showerror, e)
            end
            return ""
        end
        expand(kws) = "@macroexpand @zarr_reader(" * kws * ")"

        # `forwarding` was removed with the `:naive` branch; unknown keywords error
        @test occursin("unknown keyword",
            macro_error_msg("@zarr_reader(dtypes = (Float64,), ndims = (2,), " *
                "codecs = (ZarrCore.NoCompressor,), stores = (DirectoryStore,), " *
                "tag = TX1, forwarding = naive)"))
        @test occursin("unknown keyword",
            macro_error_msg(expand("dtypes = (Float64,), ndims = (2,), " *
                "codecs = (ZarrCore.NoCompressor,), stores = (DirectoryStore,), " *
                "forwarding = isa")))
        # ... and no `:naive` branch survives anywhere in the generated code
        @test !occursin("naive", read(joinpath(@__DIR__, "..", "src", "pools.jl"), String))

        # required keywords
        for (kws, missing_kw) in
                (("ndims = (2,), codecs = (ZarrCore.NoCompressor,), stores = (DirectoryStore,)", "dtypes"),
                 ("dtypes = (Float64,), codecs = (ZarrCore.NoCompressor,), stores = (DirectoryStore,)", "ndims"),
                 ("dtypes = (Float64,), ndims = (2,), stores = (DirectoryStore,)", "codecs"),
                 ("dtypes = (Float64,), ndims = (2,), codecs = (ZarrCore.NoCompressor,)", "stores"))
            @test occursin("`" * missing_kw * "` is required", macro_error_msg(expand(kws)))
        end
        # empty pools
        @test occursin("`codecs` pool is empty",
            macro_error_msg(expand("dtypes = (Float64,), ndims = (2,), codecs = (), " *
                                   "stores = (DirectoryStore,)")))
        # non-literal ndims
        @test occursin("integer literals",
            macro_error_msg(expand("dtypes = (Float64,), ndims = (:two,), " *
                "codecs = (ZarrCore.NoCompressor,), stores = (DirectoryStore,)")))
        # a bad `read_strategy`
        @test occursin("`read_strategy` must be",
            macro_error_msg(expand("dtypes = (Float64,), ndims = (2,), " *
                "codecs = (ZarrCore.NoCompressor,), stores = (DirectoryStore,), " *
                "read_strategy = parallel")))
        # non-`key = value` arguments
        @test occursin("expected `key = (...)` arguments",
            macro_error_msg(expand("Float64")))
        # a valid expansion, for contrast
        @test macro_error_msg(expand("dtypes = (Float64,), ndims = (2,), " *
            "codecs = (ZarrCore.NoCompressor,), stores = (DirectoryStore,)")) == ""
        @test mkreader(expand("dtypes = (Float64,), ndims = (2,), " *
            "codecs = (ZarrCore.NoCompressor,), stores = (DirectoryStore,)")) isa Expr
    end

    @testset "inference" begin
        # the visitor's leaf is fully concrete, and the whole visitor zopen is too
        @test _rt(zopen, Tuple{typeof(sum_all),typeof(TRIM_PR),String}) === Float64
        @test _rt(zopen, Tuple{typeof(sum_all),typeof(TRIM_PR),StorePool_T4,String}) === Float64
        ZT = ZArray{Float64,2,StorePool_T4,ZarrCore.MetadataV2{Float64,2,CodecPool_T4,Nothing}}
        @test _rt(_pool_leaf_T4,
                  Tuple{typeof(sum_all),Type{Float64},Val{2},StorePool_T4,String}) === Float64
        @test _rt(sum_all, Tuple{ZT}) === Float64
        @test _rt(fill_iota!, Tuple{ZT}) === Nothing
        # every generated forwarding method is concretely inferred
        @test _rt(ZarrCore.zuncompress!,
                  Tuple{Matrix{Float64},Vector{UInt8},CodecPool_T4}) === Nothing
        @test _rt(ZarrCore.zcompress!,
                  Tuple{Vector{UInt8},Matrix{Float64},CodecPool_T4}) === Nothing
        @test _rt(JSON.lower, Tuple{CodecPool_T4}) === JSON.JSONText
        @test _rt(ZarrCore.getCompressor,
                  Tuple{Type{CodecPool_T4},ZarrCore.CompressorJSON}) === CodecPool_T4
        @test _rt(ZarrCore.getCompressor, Tuple{Type{CodecPool_T4},Nothing}) === CodecPool_T4
        @test _rt(Base.getindex, Tuple{StorePool_T2,String}) === Union{Nothing,Vector{UInt8}}
        @test _rt(ZarrCore.isinitialized, Tuple{StorePool_T2,String}) === Bool
        @test _rt(ZarrCore.store_read_strategy, Tuple{StorePool_T2}) ===
              ZarrCore.SequentialRead
        # the union field is deliberately NOT concrete; that is the whole point
        @test !Base.isconcretetype(fieldtype(CodecPool_T4, :c))
        @test !Base.isconcretetype(fieldtype(StorePool_T2, :s))
        # the returning form does need the call-site assert: without it the
        # 2-variant union survives, but a 4-variant one would widen
        @test _rt(zopen, Tuple{typeof(TRIM_PN),String}) === ZArrayUnion_T2
        # `zcreate` through a pool is concrete too
        @test _rt(zcreate, Tuple{typeof(TRIM_PR),StorePool_T4,Type{Float64},NTuple{2,Int}}) ===
              ZArray{Float64,2,StorePool_T4,
                     ZarrCore.MetadataV2{Float64,2,CodecPool_T4,Nothing}}
    end
end

# ---------------------------------------------------------------------------
# Zarr v3 readers (`zarr_format` / `v3_codecs`)
# ---------------------------------------------------------------------------

@testset "@zarr_reader Zarr v3" begin
    v3path(dn, sn, cn) = joinpath(TRIM_DATADIR, string(dn, "_", sn, "_", cn, "_v3.zarr"))
    v2path(dn, sn, cn) = joinpath(TRIM_DATADIR, string(dn, "_", sn, "_", cn, ".zarr"))
    V3 = ZarrCore.Codecs.V3Codecs

    @testset "visitor zopen over all 16 v3 stores" begin
        for (dn, T) in (("f64", Float64), ("i32", Int32))
            for (sn, N, want) in (("2d", 2, 300.0), ("3d", 3, 1176.0))
                for cn in ("none", "zstd", "zlib", "blosc")
                    p = v3path(dn, sn, cn)
                    @test zopen(sum_all, TRIM_PV23, p) == want
                    @test zopen(z -> eltype(z), TRIM_PV23, p) === T
                    @test zopen(z -> ndims(z), TRIM_PV23, p) == N
                    @test zopen(z -> isconcretetype(typeof(z)), TRIM_PV23, p)
                    @test zopen(z -> typeof(z.storage), TRIM_PV23, p) === StorePool_V23
                    @test zopen(z -> typeof(z.metadata), TRIM_PV23, p) ===
                          ZarrCore.MetadataV3{T,N,P_V23,ZarrCore.ChunkKeyEncoding}
                    @test zopen(z -> eltype(z.metadata.pipeline.bytes_bytes), TRIM_PV23, p) ===
                          BBPool_V23
                    @test zopen(z -> length(z.metadata.pipeline.bytes_bytes), TRIM_PV23, p) ==
                          (cn == "none" ? 0 : 1)
                    # the explicit-store form
                    @test zopen(sum_all, TRIM_PV23, StorePool_V23(DirectoryStore(p)), "") == want
                end
            end
        end
        # the same reader still reads all 16 v2 stores
        for (dn, _) in (("f64", Float64), ("i32", Int32))
            for (sn, want) in (("2d", 300.0), ("3d", 1176.0))
                for cn in ("none", "zstd", "zlib", "blosc")
                    @test zopen(sum_all, TRIM_PV23, v2path(dn, sn, cn)) == want
                    @test zopen(z -> typeof(z.metadata.compressor), TRIM_PV23, v2path(dn, sn, cn)) ===
                          CodecPool_V23
                end
            end
        end
    end

    @testset "a three-codec chain [bytes, zstd, crc32c]" begin
        @test zopen(sum_all, TRIM_PV23, TRIM_CRC_PATH) == 300.0
        @test zopen(z -> length(z.metadata.pipeline.bytes_bytes), TRIM_PV23, TRIM_CRC_PATH) == 2
        @test zopen(z -> V3.name(z.metadata.pipeline.bytes_bytes[1]), TRIM_PV23, TRIM_CRC_PATH) == "zstd"
        @test zopen(z -> V3.name(z.metadata.pipeline.bytes_bytes[2]), TRIM_PV23, TRIM_CRC_PATH) == "crc32c"
    end

    @testset "returning zopen over both formats" begin
        z2 = zopen(TRIM_PV3N, v2path("f64", "2d", "zstd"))::ZArrayUnion_V3N
        @test z2 isa ZArray{Float64,2,StorePool_V3N,
                            ZarrCore.MetadataV2{Float64,2,CodecPool_V3N,Nothing}}
        z3 = zopen(TRIM_PV3N, v3path("f64", "2d", "zstd"))::ZArrayUnion_V3N
        @test z3 isa ZArray{Float64,2,StorePool_V3N,
                            ZarrCore.MetadataV3{Float64,2,P_V3N,ZarrCore.ChunkKeyEncoding}}
        @test sum_all(z2) == 300.0
        @test sum_all(z3) == 300.0
        # the union has both members
        @test ZArrayUnion_V3N isa Union
        # 8 union members -> the wide reader refuses the returning form
        @test_throws ArgumentError zopen(TRIM_PV23, v3path("f64", "2d", "zstd"))
    end

    @testset "v3-only reader: no v2 codec pool, no zcreate" begin
        @test zopen(sum_all, TRIM_PV3, v3path("f64", "2d", "zstd")) == 300.0
        @test !(@isdefined CodecPool_V3O)
        @test !any(m -> Base.unwrap_unionall(m.sig).parameters[2] === typeof(TRIM_PV3),
                    methods(zcreate))
        # ... while a reader that does have a `codecs` pool keeps them
        @test any(m -> Base.unwrap_unionall(m.sig).parameters[2] === typeof(TRIM_PV23),
                   methods(zcreate))
        # a v2 store is invisible to a v3-only reader, and vice versa
        @test_throws ArgumentError zopen(sum_all, TRIM_PV3, v2path("f64", "2d", "zstd"))
        @test_throws ArgumentError zopen(sum_all, TRIM_PR, v3path("f64", "2d", "zstd"))
    end

    @testset "pool mismatch errors" begin
        # a zstd v3 store, but the v3 pool only has blosc
        @test_throws ArgumentError zopen(sum_all, TRIM_PV3B, v3path("f64", "2d", "zstd"))
        # a transpose codec: the pipeline's array->array stage is `Tuple{}`
        @test_throws ArgumentError zopen(sum_all, TRIM_PV23, TRIM_TRANSPOSE_PATH)
        # dtype / rank outside the pool
        @test_throws ArgumentError zopen(sum_all, TRIM_PV3, v3path("i32", "2d", "zstd"))
        @test_throws ArgumentError zopen(sum_all, TRIM_PV3, v3path("f64", "3d", "zstd"))
        # neither .zarray nor zarr.json, and the message names both
        @test_throws ArgumentError zopen(sum_all, TRIM_PV23, TRIM_DATADIR)
        msg = try
            zopen(sum_all, TRIM_PV23, TRIM_DATADIR)
            ""
        catch e
            sprint(showerror, e)
        end
        @test occursin(".zarray", msg)
        @test occursin("zarr.json", msg)
        # a single-format reader names only its own document
        msg3 = try
            zopen(sum_all, TRIM_PV3, TRIM_DATADIR)
            ""
        catch e
            sprint(showerror, e)
        end
        @test occursin("zarr.json", msg3)
        @test !occursin(".zarray", msg3)
    end

    @testset "BBPool forwarding in isolation" begin
        ctx = (shape = [4, 6], elsize = 8)
        CJ = ZarrCore.CodecJSON
        for (nm, M, inst) in (("zstd", ZarrZstd.ZstdV3Codec, ZarrZstd.ZstdV3Codec(3)),
                              ("gzip", ZarrZlib.GzipV3Codec, ZarrZlib.GzipV3Codec(6)),
                              ("blosc", ZarrBlosc.BloscV3Codec, ZarrBlosc.BloscV3Codec()),
                              ("crc32c", ZarrCore.CRC32cV3Codec, ZarrCore.CRC32cV3Codec()))
            p = V3.getCodec(BBPool_V23, CJ(nm), ctx)
            @test p isa BBPool_V23
            @test p.c isa M
            @test V3.name(p) == nm
            @test JSON.lower(p) isa JSON.JSONText
            @test ZarrTrimmable._json_string(JSON.lower(p)) ==
                  ZarrTrimmable._json_string(JSON.lower(p.c))
            # every member survives an encode/decode round trip through the pool
            data = Vector{UInt8}(codeunits(repeat("abcdefgh", 16)))
            pool = BBPool_V23(inst)
            @test V3.codec_decode(pool, V3.codec_encode(pool, data)) == data
        end
        # a "numcodecs." prefix is stripped before the chain
        @test V3.getCodec(BBPool_V23, CJ("numcodecs.zstd"), ctx).c isa ZarrZstd.ZstdV3Codec
        @test_throws ArgumentError V3.getCodec(BBPool_V23, CJ("lzma"), ctx)
        @test BBPool_V23 <: V3.V3Codec{:bytes,:bytes}
        # the union field is deliberately NOT concrete, as for the v2 pool
        @test !Base.isconcretetype(fieldtype(BBPool_V23, :c))
        # a one-member pool's field is concrete, and that is fine
        @test fieldtype(BBPool_V3O, :c) === ZarrZstd.ZstdV3Codec
        @test P_V23 === ZarrCore.V3Pipeline{Tuple{},ZarrCore.BytesCodec,Vector{BBPool_V23}}
    end

    @testset "inference" begin
        @test _rt(zopen, Tuple{typeof(sum_all),typeof(TRIM_PV23),String}) === Float64
        @test _rt(zopen, Tuple{typeof(sum_all),typeof(TRIM_PV23),StorePool_V23,String}) === Float64
        @test _rt(V3.codec_encode, Tuple{BBPool_V23,Vector{UInt8}}) === Vector{UInt8}
        @test _rt(V3.codec_decode, Tuple{BBPool_V23,Vector{UInt8}}) === Vector{UInt8}
        @test _rt(JSON.lower, Tuple{BBPool_V23}) === JSON.JSONText
        @test _rt(V3.name, Tuple{BBPool_V23}) === String
        @test _rt(V3.getCodec, Tuple{Type{BBPool_V23},ZarrCore.CodecJSON,
                                     NamedTuple{(:shape, :elsize),Tuple{Vector{Int},Int}}}) ===
              BBPool_V23
        ZT3 = ZArray{Float64,2,StorePool_V23,
                     ZarrCore.MetadataV3{Float64,2,P_V23,ZarrCore.ChunkKeyEncoding}}
        @test _rt(_pool_leaf3_V23,
                  Tuple{typeof(sum_all),Type{Float64},Val{2},StorePool_V23,String}) === Float64
        @test _rt(sum_all, Tuple{ZT3}) === Float64
        @test _rt(zopen, Tuple{typeof(TRIM_PV3N),String}) === ZArrayUnion_V3N
    end

    @testset "macro argument errors (v3)" begin
        function macro_error_msg(s::String)
            try
                eval(Meta.parse(s))
            catch e
                e isa LoadError && (e = e.error)
                return sprint(showerror, e)
            end
            return ""
        end
        expand(kws) = "@macroexpand @zarr_reader(" * kws * ")"
        base = "dtypes = (Float64,), ndims = (2,), stores = (DirectoryStore,)"

        # 3 in `zarr_format` without a `v3_codecs` pool
        @test occursin("`v3_codecs` pool is empty",
            macro_error_msg(expand(base * ", zarr_format = (3,)")))
        @test occursin("`v3_codecs` pool is empty",
            macro_error_msg(expand(base * ", zarr_format = (3,), v3_codecs = ()")))
        # `v3_codecs` without 3 in `zarr_format`
        @test occursin("`v3_codecs` was given but 3 is not in `zarr_format`",
            macro_error_msg(expand(base * ", codecs = (ZarrCore.NoCompressor,), " *
                                   "v3_codecs = (ZarrZstd.ZstdV3Codec,)")))
        # a format outside (2, 3)
        @test occursin("`zarr_format` must be drawn from",
            macro_error_msg(expand(base * ", zarr_format = (1,)")))
        # a repeated entry
        @test occursin("repeated entry",
            macro_error_msg(expand(base * ", codecs = (ZarrCore.NoCompressor,), " *
                                   "zarr_format = (2, 2)")))
        # non-literal formats
        @test occursin("`zarr_format` must be integer literals",
            macro_error_msg(expand(base * ", zarr_format = (:v3,)")))
        # `codecs` is still required when 2 is in the pool ...
        @test occursin("`codecs` is required",
            macro_error_msg(expand(base * ", zarr_format = (2, 3), " *
                                   "v3_codecs = (ZarrZstd.ZstdV3Codec,)")))
        # ... and optional when it is not
        @test macro_error_msg(expand(base * ", zarr_format = (3,), " *
                                     "v3_codecs = (ZarrZstd.ZstdV3Codec,)")) == ""
        # a bare integer is accepted for `zarr_format`
        @test macro_error_msg(expand(base * ", zarr_format = 3, " *
                                     "v3_codecs = (ZarrZstd.ZstdV3Codec,)")) == ""
        @test macro_error_msg(expand(base * ", codecs = (ZarrCore.NoCompressor,), " *
                                     "zarr_format = 2")) == ""
    end
end
