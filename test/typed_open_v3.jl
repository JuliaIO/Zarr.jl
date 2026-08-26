# Tests for the statically typed `zopen(::Type{T}, ::Val{N}, ...; pipeline = P)`
# path (Zarr v3).
#
# This path parses `zarr.json` into the concrete `ZarrJsonV3` schema and
# validates the stored document against the caller-supplied `T`/`N`/`P`, so the
# returned `ZArray` is fully concretely typed (which is what makes it juliac
# `--trim=safe`). The tests below check the round trips, the validation errors
# and the schema-level helpers, mirroring `typed_open.jl` for v2.

using Zarr: NoCompressor, ZstdCompressor, ZlibCompressor, BloscCompressor,
    ZstdV3Codec, GzipV3Codec, BloscV3Codec, DictStore, DirectoryStore
using Zarr.Codecs.V3Codecs: BytesCodec, CRC32cV3Codec, TransposeCodec,
    ShardingCodec, VLenUTF8V3Codec

const V3P = ZarrCore.V3Pipeline
const CKE = ZarrCore.ChunkKeyEncoding

# The pipeline types a caller has to name for the typed open.
const P_BYTES = V3P{Tuple{},BytesCodec,Tuple{}}
const P_ZSTD  = V3P{Tuple{},BytesCodec,Tuple{ZstdV3Codec}}
const P_GZIP  = V3P{Tuple{},BytesCodec,Tuple{GzipV3Codec}}
const P_BLOSC = V3P{Tuple{},BytesCodec,Tuple{BloscV3Codec}}
const P_CRC   = V3P{Tuple{},BytesCodec,Tuple{CRC32cV3Codec}}

md3type(::Type{T}, N, ::Type{P}) where {T,P} = ZarrCore.MetadataV3{T,N,P,CKE}

# Write `data` through a hand-built `MetadataV3` (used for the codec
# configurations `zcreate` cannot express).
function write_handbuilt(store, path, md, data)
    z = Zarr.ZArray(md, store, path, Dict{String,Any}(), true)
    ZarrCore.writemetadata(ZarrCore.zarr_format(md), store, path, md)
    z[(Colon() for _ in 1:ndims(data))...] = data
    return z
end

@testset "Typed open v3" begin

    @testset "round trip bytes only" begin
        p = mktempdir()
        data = reshape(collect(1.0:24.0), 4, 6)
        z0 = zcreate(Float64, DirectoryStore(p), 4, 6; chunks=(2, 3), zarr_format=3,
                     compressor=NoCompressor(), fill_value=0.0)
        z0[:, :] = data

        z = zopen(Float64, Val(2), DirectoryStore(p), ""; pipeline=P_BYTES)
        @test z isa ZArray{Float64,2,DirectoryStore,md3type(Float64, 2, P_BYTES)}
        @test eltype(z) === Float64
        @test size(z) == (4, 6)
        @test z[:, :] == data
        @test z[2:3, 4:6] == data[2:3, 4:6]

        dyn = zopen(p)
        @test z.metadata == dyn.metadata
        @test z.metadata.chunk_key_encoding == CKE('/', true)
        @test z.metadata.dtype == "float64"
        @test z.attrs isa Dict{String,Any}
        @test isempty(z.attrs)
        @test z.path == ""
        @test !z.writeable

        # `path::AbstractString` convenience method (wraps a DirectoryStore).
        z2 = zopen(Float64, Val(2), p; pipeline=P_BYTES)
        @test z2 isa ZArray{Float64,2,DirectoryStore,md3type(Float64, 2, P_BYTES)}
        @test z2[:, :] == data

        # A store-generic path: the same code with a DictStore.
        ds = DictStore()
        zd0 = zcreate(Float64, ds, 3, 2; chunks=(3, 1), zarr_format=3,
                      compressor=NoCompressor(), fill_value=0.0)
        ddata = [1.0 2.0; 3.0 4.0; 5.0 6.0]
        zd0[:, :] = ddata
        zd = zopen(Float64, Val(2), ds, ""; pipeline=P_BYTES)
        @test zd isa ZArray{Float64,2,DictStore,md3type(Float64, 2, P_BYTES)}
        @test zd[:, :] == ddata
    end

    @testset "round trip zstd" begin
        p = mktempdir()
        data = reshape(collect(1.0:24.0), 4, 6)
        z0 = zcreate(Float64, DirectoryStore(p), 4, 6; chunks=(2, 3), zarr_format=3,
                     compressor=ZstdCompressor(level=3), fill_value=0.0)
        z0[:, :] = data

        z = zopen(Float64, Val(2), DirectoryStore(p), ""; pipeline=P_ZSTD)
        @test z isa ZArray{Float64,2,DirectoryStore,md3type(Float64, 2, P_ZSTD)}
        @test z[:, :] == data
        @test z.metadata.pipeline.bytes_bytes[1] == ZstdV3Codec(3)
        @test z.metadata == zopen(p).metadata
    end

    @testset "round trip gzip" begin
        p = mktempdir()
        data = reshape(collect(Int32(1):Int32(12)), 3, 4)
        z0 = zcreate(Int32, DirectoryStore(p), 3, 4; chunks=(3, 2), zarr_format=3,
                     compressor=ZlibCompressor(), fill_value=Int32(0))
        z0[:, :] = data

        z = zopen(Int32, Val(2), DirectoryStore(p), ""; pipeline=P_GZIP)
        @test z isa ZArray{Int32,2,DirectoryStore,md3type(Int32, 2, P_GZIP)}
        @test z[:, :] == data
        @test z.metadata.pipeline.bytes_bytes[1] isa GzipV3Codec
        @test z.metadata == zopen(p).metadata
    end

    @testset "round trip blosc" begin
        p = mktempdir()
        data = reshape(collect(1.0:24.0), 4, 6)
        z0 = zcreate(Float64, DirectoryStore(p), 4, 6; chunks=(2, 3), zarr_format=3,
                     compressor=BloscCompressor(), fill_value=0.0)
        z0[:, :] = data

        z = zopen(Float64, Val(2), DirectoryStore(p), ""; pipeline=P_BLOSC)
        @test z isa ZArray{Float64,2,DirectoryStore,md3type(Float64, 2, P_BLOSC)}
        @test z[:, :] == data
        bc = z.metadata.pipeline.bytes_bytes[1]
        @test bc.cname == "lz4"
        @test bc.typesize == 8
        @test z.metadata == zopen(p).metadata
    end

    @testset "other ranks" begin
        p1 = mktempdir()
        d1 = collect(Int64(1):Int64(7))
        z1c = zcreate(Int64, DirectoryStore(p1), 7; chunks=(3,), zarr_format=3,
                      compressor=NoCompressor(), fill_value=Int64(0))
        z1c[:] = d1
        z1 = zopen(Int64, Val(1), DirectoryStore(p1), ""; pipeline=P_BYTES)
        @test z1 isa ZArray{Int64,1,DirectoryStore,md3type(Int64, 1, P_BYTES)}
        @test z1[:] == d1

        p3 = mktempdir()
        d3 = reshape(collect(Float32(1):Float32(24)), 2, 3, 4)
        z3c = zcreate(Float32, DirectoryStore(p3), 2, 3, 4; chunks=(2, 3, 2),
                      zarr_format=3, compressor=ZstdCompressor(level=3),
                      fill_value=Float32(0))
        z3c[:, :, :] = d3
        z3 = zopen(Float32, Val(3), DirectoryStore(p3), ""; pipeline=P_ZSTD)
        @test z3 isa ZArray{Float32,3,DirectoryStore,md3type(Float32, 3, P_ZSTD)}
        @test z3[:, :, :] == d3
        @test z3[1:2, 2:3, 3:4] == d3[1:2, 2:3, 3:4]
    end

    @testset "transpose codec (F order)" begin
        p = mktempdir()
        store = DirectoryStore(p)
        data = reshape(collect(1.0:12.0), 3, 4)
        md = ZarrCore.Metadata3(data, (3, 2); compressor=NoCompressor(),
                                fill_value=0.0, order='F')
        write_handbuilt(store, "", md, data)

        PT = V3P{Tuple{TransposeCodec{2}},BytesCodec,Tuple{}}
        z = zopen(Float64, Val(2), DirectoryStore(p), ""; pipeline=PT)
        @test z isa ZArray{Float64,2,DirectoryStore,md3type(Float64, 2, PT)}
        @test z.metadata.pipeline.array_array[1].order == (2, 1)
        @test z[:, :] == data
        @test z[2:3, 1:2] == data[2:3, 1:2]
        @test z.metadata == zopen(p).metadata
    end

    @testset "crc32c codec" begin
        p = mktempdir()
        store = DirectoryStore(p)
        data = reshape(collect(1.0:12.0), 3, 4)
        pipeline = V3P((), BytesCodec(:little), (CRC32cV3Codec(),))
        md = ZarrCore.MetadataV3{Float64,2,typeof(pipeline)}(
            3, "array", (3, 4), (3, 2), ZarrCore.typestr3(Float64), pipeline, 0.0,
            CKE('/', true))
        write_handbuilt(store, "", md, data)

        z = zopen(Float64, Val(2), DirectoryStore(p), ""; pipeline=P_CRC)
        @test z isa ZArray{Float64,2,DirectoryStore,md3type(Float64, 2, P_CRC)}
        @test z[:, :] == data
        @test z.metadata == zopen(p).metadata
    end

    @testset "chunk key encoding v2" begin
        p = mktempdir()
        store = DirectoryStore(p)
        data = reshape(collect(1.0:12.0), 3, 4)
        pipeline = V3P((), BytesCodec(:little), ())
        md = ZarrCore.MetadataV3{Float64,2,typeof(pipeline)}(
            3, "array", (3, 4), (3, 2), ZarrCore.typestr3(Float64), pipeline, 0.0,
            CKE('.', false))
        write_handbuilt(store, "", md, data)
        @test isfile(joinpath(p, "0.0"))

        z = zopen(Float64, Val(2), DirectoryStore(p), ""; pipeline=P_BYTES)
        @test z.metadata.chunk_key_encoding == CKE('.', false)
        @test z[:, :] == data

        # The "default" name keeps the prefix but honours a custom separator.
        @test ZarrCore._chunk_key_encoding_typed(
            ZarrCore.ChunkKeyEncodingJSON("default", nothing)) == CKE('/', true)
        @test ZarrCore._chunk_key_encoding_typed(
            ZarrCore.ChunkKeyEncodingJSON("default",
                ZarrCore.ChunkKeyEncodingConfigJSON("."))) == CKE('.', true)
        @test ZarrCore._chunk_key_encoding_typed(
            ZarrCore.ChunkKeyEncodingJSON("v2", nothing)) == CKE('.', false)
        @test_throws ArgumentError ZarrCore._chunk_key_encoding_typed(
            ZarrCore.ChunkKeyEncodingJSON("suffix", nothing))
    end

    @testset "errors" begin
        p = mktempdir()
        data = reshape(collect(1.0:24.0), 4, 6)
        z0 = zcreate(Float64, DirectoryStore(p), 4, 6; chunks=(2, 3), zarr_format=3,
                     compressor=NoCompressor(), fill_value=0.0)
        z0[:, :] = data

        # dtype mismatch (both directions).
        @test_throws ArgumentError zopen(Float32, Val(2), DirectoryStore(p), "";
                                         pipeline=P_BYTES)
        @test_throws ArgumentError zopen(Int64, Val(2), DirectoryStore(p), "";
                                         pipeline=P_BYTES)
        # ndims mismatch.
        @test_throws ArgumentError zopen(Float64, Val(1), DirectoryStore(p), "";
                                         pipeline=P_BYTES)
        @test_throws ArgumentError zopen(Float64, Val(3), DirectoryStore(p), "";
                                         pipeline=P_BYTES)
        # codec count mismatch: the store has one codec, the pipeline wants two.
        @test_throws ArgumentError zopen(Float64, Val(2), DirectoryStore(p), "";
                                         pipeline=P_ZSTD)
        # ... and a transpose the store does not have.
        @test_throws ArgumentError zopen(Float64, Val(2), DirectoryStore(p), "";
            pipeline=V3P{Tuple{TransposeCodec{2}},BytesCodec,Tuple{}})

        # codec name mismatch: zstd on disk, gzip requested.
        pz = mktempdir()
        zz = zcreate(Float64, DirectoryStore(pz), 4, 6; chunks=(2, 3), zarr_format=3,
                     compressor=ZstdCompressor(level=3), fill_value=0.0)
        zz[:, :] = data
        @test_throws ArgumentError zopen(Float64, Val(2), DirectoryStore(pz), "";
                                         pipeline=P_GZIP)
        @test_throws ArgumentError zopen(Float64, Val(2), DirectoryStore(pz), "";
                                         pipeline=P_BYTES)

        # No `zarr.json` at all.
        @test_throws ArgumentError zopen(Float64, Val(2), DictStore(), "";
                                         pipeline=P_BYTES)
        pe = mktempdir()
        @test_throws ArgumentError zopen(Float64, Val(2), DirectoryStore(pe), "";
                                         pipeline=P_BYTES)
        # Leading '/'.
        @test_throws ArgumentError zopen(Float64, Val(2), DirectoryStore(p), "/sub";
                                         pipeline=P_BYTES)

        # Exactly one of `compressor` / `pipeline`.
        @test_throws ArgumentError zopen(Float64, Val(2), DirectoryStore(p), "")
        @test_throws ArgumentError zopen(Float64, Val(2), DirectoryStore(p), "";
                                         compressor=NoCompressor, pipeline=P_BYTES)
        @test_throws ArgumentError zopen(Float64, Val(2), p)

        # A v2 store cannot be opened with a v3 pipeline and vice versa.
        p2 = mktempdir()
        z2 = zcreate(Float64, DirectoryStore(p2), 4, 6; chunks=(2, 3),
                     compressor=NoCompressor(), fill_value=0.0)
        z2[:, :] = data
        @test_throws ArgumentError zopen(Float64, Val(2), DirectoryStore(p2), "";
                                         pipeline=P_BYTES)
        @test_throws ArgumentError zopen(Float64, Val(2), DirectoryStore(p), "";
                                         compressor=NoCompressor)

        # Sharding and vlen-utf8 have explicit "unsupported" methods.
        ctx = (shape = [4], elsize = 2)
        @test_throws ArgumentError ZarrCore.Codecs.V3Codecs.getCodec(
            ShardingCodec, ZarrCore.CodecJSON("sharding_indexed"), ctx)
        @test_throws ArgumentError ZarrCore.Codecs.V3Codecs.getCodec(
            VLenUTF8V3Codec, ZarrCore.CodecJSON("vlen-utf8"), ctx)

        # Documents that the schema parses but the typed metadata rejects.
        base = """{"zarr_format":3,"node_type":"array","shape":[4],"data_type":"int16",
            "chunk_grid":{"name":"regular","configuration":{"chunk_shape":[4]}},
            "chunk_key_encoding":{"name":"NAME"},"fill_value":FILL,
            "codecs":[{"name":"bytes","configuration":{"endian":"little"}}]EXTRA}"""
        doc(name, fv, extra) = replace(replace(replace(base, "NAME" => name),
            "FILL" => fv), "EXTRA" => extra)

        mk(s) = (d = DictStore(); d["zarr.json"] = Vector{UInt8}(s); d)
        # suffix chunk key encoding
        @test_throws ArgumentError zopen(Int16, Val(1), mk(doc("suffix", "0", "")), "";
                                         pipeline=P_BYTES)
        # non-empty storage_transformers
        @test_throws ArgumentError zopen(Int16, Val(1),
            mk(doc("default", "0", ""","storage_transformers":[{"name":"x"}]""")), "";
            pipeline=P_BYTES)
        # ... but an empty list is fine (that is what zarr-python writes).
        zst = zopen(Int16, Val(1),
            mk(doc("default", "0", ""","storage_transformers":[]""")), "";
            pipeline=P_BYTES)
        @test zst isa ZArray{Int16,1,DictStore,md3type(Int16, 1, P_BYTES)}
        # null fill value
        @test_throws ArgumentError zopen(Int16, Val(1), mk(doc("default", "null", "")), "";
                                         pipeline=P_BYTES)
        # non-regular chunk grid
        @test_throws ArgumentError zopen(Int16, Val(1),
            mk(replace(doc("default", "0", ""), "\"regular\"" => "\"rectangular\"")), "";
            pipeline=P_BYTES)
        # node_type / zarr_format
        @test_throws ArgumentError zopen(Int16, Val(1),
            mk(replace(doc("default", "0", ""), "\"array\"" => "\"group\"")), "";
            pipeline=P_BYTES)
        @test_throws ArgumentError zopen(Int16, Val(1),
            mk(replace(doc("default", "0", ""), "\"zarr_format\":3" => "\"zarr_format\":2")), "";
            pipeline=P_BYTES)
    end

    @testset "fill_as_missing" begin
        p = mktempdir()
        z0 = zcreate(Float64, DirectoryStore(p), 2, 4; chunks=(2, 2), zarr_format=3,
                     compressor=NoCompressor(), fill_value=-1.0)
        z0[:, 1:2] = [1.0 2.0; 3.0 4.0]

        z = zopen(Float64, Val(2), DirectoryStore(p), ""; pipeline=P_BYTES,
                  fill_as_missing=true)
        @test z isa ZArray{Union{Float64,Missing},2,DirectoryStore,
                           md3type(Union{Float64,Missing}, 2, P_BYTES)}
        a = z[:, :]
        @test a[:, 1:2] == [1.0 2.0; 3.0 4.0]
        @test all(ismissing, a[:, 3:4])

        # Without the keyword the sentinel comes back as a plain value.
        zn = zopen(Float64, Val(2), DirectoryStore(p), ""; pipeline=P_BYTES)
        @test zn isa ZArray{Float64,2,DirectoryStore,md3type(Float64, 2, P_BYTES)}
        @test zn[:, 3:4] == fill(-1.0, 2, 2)

        # Int element type.
        pi_ = mktempdir()
        zi0 = zcreate(Int32, DirectoryStore(pi_), 4; chunks=(2,), zarr_format=3,
                      compressor=NoCompressor(), fill_value=Int32(-9))
        zi0[1:2] = Int32[7, 8]
        zi = zopen(Int32, Val(1), DirectoryStore(pi_), ""; pipeline=P_BYTES,
                   fill_as_missing=true)
        @test zi isa ZArray{Union{Int32,Missing},1,DirectoryStore,
                            md3type(Union{Int32,Missing}, 1, P_BYTES)}
        ai = zi[:]
        @test ai[1:2] == Int32[7, 8]
        @test all(ismissing, ai[3:4])
    end

    @testset "attrs" begin
        p = mktempdir()
        atts = Dict{String,Any}("nested" => Dict{String,Any}("k" => "v"),
                                "n" => 3, "f" => 1.5)
        z0 = zcreate(Float64, DirectoryStore(p), 2, 2; chunks=(2, 2), zarr_format=3,
                     compressor=NoCompressor(), fill_value=0.0, attrs=atts)
        z0[:, :] = [1.0 2.0; 3.0 4.0]

        z = zopen(Float64, Val(2), DirectoryStore(p), ""; pipeline=P_BYTES)
        @test z.attrs isa Dict{String,Any}
        @test z.attrs["n"] === 3          # integers stay integers
        @test z.attrs["f"] === 1.5
        @test z.attrs["nested"]["k"] == "v"

        # Empty / absent attributes.
        pe = mktempdir()
        ze0 = zcreate(Float64, DirectoryStore(pe), 2; chunks=(2,), zarr_format=3,
                      compressor=NoCompressor(), fill_value=0.0)
        ze0[:] = [1.0, 2.0]
        @test isempty(ZarrCore.getattrs_typed(ZarrCore.ZarrFormat(Val(3)), DirectoryStore(pe), ""))
        @test isempty(ZarrCore.getattrs_typed(ZarrCore.ZarrFormat(Val(3)), DictStore(), ""))
    end

    @testset "parse_zarrjson" begin
        s = """{"zarr_format":3,"node_type":"array","shape":[4,6],"data_type":"float64",
            "chunk_grid":{"name":"regular","configuration":{"chunk_shape":[2,3]}},
            "chunk_key_encoding":{"name":"default","configuration":{"separator":"/"}},
            "fill_value":0.0,
            "codecs":[{"name":"bytes","configuration":{"endian":"big"}},
                      {"name":"blosc","configuration":{"cname":"zstd","clevel":5,
                       "shuffle":"noshuffle","blocksize":0,"typesize":4}}],
            "attributes":{"a":1},"dimension_names":["y","x"],
            "storage_transformers":[]}"""
        z = ZarrCore.parse_zarrjson(s)
        @test z isa ZarrCore.ZarrJsonV3
        @test z.zarr_format == 3
        @test z.node_type == "array"
        @test z.shape == [4, 6]
        @test z.data_type == "float64"
        g = ZarrCore.chunk_grid_json(z)
        @test g isa ZarrCore.ChunkGridJSON
        @test g.name == "regular"
        @test ZarrCore._json_int_vector(g.configuration.chunk_shape) == [2, 3]
        e = ZarrCore.chunk_key_encoding_json(z)
        @test e isa ZarrCore.ChunkKeyEncodingJSON
        @test e.name == "default"
        @test e.configuration.separator == "/"
        @test ZarrCore.fill_value_typed(Float64, z.fill_value) === 0.0
        cs = ZarrCore.codecs_json(z)
        @test cs isa Vector{ZarrCore.CodecJSON}
        @test length(cs) == 2
        @test cs[1].name == "bytes"
        @test cs[1].configuration.endian == "big"
        @test cs[2].configuration.cname == "zstd"
        @test cs[2].configuration.shuffle == "noshuffle"
        @test cs[2].configuration.typesize == 4
        @test z.storage_transformers == JSON.JSONText[]
        # Byte-vector method.
        @test ZarrCore.parse_zarrjson(Vector{UInt8}(s)).shape == [4, 6]
        # Absent keys keep their defaults.
        z2 = ZarrCore.parse_zarrjson("""{"zarr_format":3}""")
        @test z2.node_type == ""
        @test isempty(z2.codecs)
        @test isempty(ZarrCore.codecs_json(z2))
        @test_throws ArgumentError ZarrCore.chunk_grid_json(z2)
        @test_throws ArgumentError ZarrCore.chunk_key_encoding_json(z2)
        @test z2.storage_transformers === nothing
        @test ZarrCore.isnullfill(z2.fill_value)
        # An object-valued data_type is rejected at parse time.
        @test_throws Exception ZarrCore.parse_zarrjson(
            """{"zarr_format":3,"data_type":{"name":"fixed_length_utf32"}}""")

        # Keyword constructor.
        c = ZarrCore.CodecJSON("zstd"; level=7)
        @test c.name == "zstd"
        @test c.configuration.level == 7
        @test ZarrCore.CodecJSON("crc32c").configuration === nothing
        @test ZarrCore.CodecJSON().name == ""
        @test ZarrCore.CodecJSON().configuration === nothing
    end

    @testset "typed getCodec" begin
        getCodec = ZarrCore.Codecs.V3Codecs.getCodec
        CJ = ZarrCore.CodecJSON
        ctx = (shape = [4, 6], elsize = 8)

        @test getCodec(BytesCodec, CJ("bytes"; endian="big"), ctx) == BytesCodec(:big)
        @test getCodec(BytesCodec, CJ("bytes"), ctx) == BytesCodec(:little)
        @test_throws ArgumentError getCodec(BytesCodec, CJ("bytes"; endian="middle"), ctx)
        @test_throws ArgumentError getCodec(BytesCodec, CJ("zstd"), ctx)

        # `numcodecs.`-prefixed names are accepted.
        @test getCodec(ZstdV3Codec, CJ("numcodecs.zstd"; level=9), ctx) == ZstdV3Codec(9)
        @test getCodec(ZstdV3Codec, CJ("zstd"), ctx) == ZstdV3Codec(3)
        @test getCodec(GzipV3Codec, CJ("gzip"), ctx) == GzipV3Codec(6)
        @test getCodec(GzipV3Codec, CJ("gzip"; level=1), ctx) == GzipV3Codec(1)
        @test getCodec(CRC32cV3Codec, CJ("crc32c"), ctx) == CRC32cV3Codec()

        b = getCodec(BloscV3Codec, CJ("blosc"), ctx)
        @test b == BloscV3Codec("lz4", 5, 0, 0, 8)   # `typesize` from the context
        @test getCodec(BloscV3Codec, CJ("blosc"; cname="zstd", clevel=3,
            shuffle="bitshuffle", blocksize=16, typesize=2), ctx) ==
            BloscV3Codec("zstd", 3, 2, 16, 2)
        @test_throws ArgumentError getCodec(BloscV3Codec, CJ("blosc"; shuffle="nope"), ctx)

        t = getCodec(TransposeCodec{3}, CJ("transpose"; order=[2, 1, 0]), ctx)
        @test t == TransposeCodec((3, 2, 1))
        @test_throws ArgumentError getCodec(TransposeCodec{2}, CJ("transpose";
            order=[2, 1, 0]), ctx)
        @test_throws ArgumentError getCodec(TransposeCodec{2}, CJ("transpose"), ctx)

        # `codec_name` is the static counterpart of `name`.
        codec_name = ZarrCore.Codecs.V3Codecs.codec_name
        @test codec_name(BytesCodec) == "bytes"
        @test codec_name(TransposeCodec{2}) == "transpose"
        @test codec_name(CRC32cV3Codec) == "crc32c"
        @test codec_name(VLenUTF8V3Codec) == "vlen-utf8"
        @test codec_name(ZstdV3Codec) == "zstd"
        @test codec_name(GzipV3Codec) == "gzip"
        @test codec_name(BloscV3Codec) == "blosc"
        @test ZarrCore.Codecs.V3Codecs._strip_numcodecs("numcodecs.zstd") == "zstd"
        @test ZarrCore.Codecs.V3Codecs._strip_numcodecs("zstd") == "zstd"
    end

    @testset "pipeline_from_json" begin
        pipeline_from_json = ZarrCore.Codecs.V3Codecs.pipeline_from_json
        CJ = ZarrCore.CodecJSON
        ctx = (shape = [4], elsize = 8)

        cs = ZarrCore.CodecJSON[CJ("bytes"; endian="little"), CJ("zstd"; level=5)]
        p = pipeline_from_json(P_ZSTD, cs, ctx)
        @test p isa P_ZSTD
        @test p.bytes_bytes[1] == ZstdV3Codec(5)
        @test_throws ArgumentError pipeline_from_json(P_BYTES, cs, ctx)

        # A `Vector` bytes->bytes stage accepts any number of trailing codecs.
        PV = V3P{Tuple{},BytesCodec,Vector{ZstdV3Codec}}
        pv = pipeline_from_json(PV, cs, ctx)
        @test pv isa PV
        @test pv.bytes_bytes == [ZstdV3Codec(5)]
        pv0 = pipeline_from_json(PV, ZarrCore.CodecJSON[CJ("bytes")], ctx)
        @test isempty(pv0.bytes_bytes)
        @test_throws ArgumentError pipeline_from_json(PV, ZarrCore.CodecJSON[], ctx)
    end

    @testset "writing through a typed handle" begin
        p = mktempdir()
        z0 = zcreate(Float64, DirectoryStore(p), 2, 2; chunks=(2, 2), zarr_format=3,
                     compressor=NoCompressor(), fill_value=0.0)

        zw = zopen(Float64, Val(2), DirectoryStore(p), ""; pipeline=P_BYTES, mode="w")
        @test zw isa ZArray{Float64,2,DirectoryStore,md3type(Float64, 2, P_BYTES)}
        @test zw.writeable
        zw[1, 1] = 42.0
        zw[2, 2] = -3.0
        @test zopen(p)[:, :] == [42.0 0.0; 0.0 -3.0]
        @test zopen(Float64, Val(2), DirectoryStore(p), ""; pipeline=P_BYTES)[1, 1] == 42.0

        zr = zopen(Float64, Val(2), DirectoryStore(p), ""; pipeline=P_BYTES)
        @test !zr.writeable
        @test_throws ErrorException zr[1, 1] = 1.0

        # ... and through the `path` convenience method.
        zw2 = zopen(Float64, Val(2), p; pipeline=P_BYTES, mode="w")
        zw2[1, 2] = 7.0
        @test zopen(p)[1, 2] == 7.0
    end

    @testset "python fixtures" begin
        fixture_path = joinpath(@__DIR__, "v3_python", "data.zarr")
        if !isdir(fixture_path)
            @warn "Python v3 fixtures not found at $fixture_path, skipping"
        else
            store = DirectoryStore(fixture_path)

            z = zopen(Int16, Val(1), store, "1d.contiguous.raw.i2"; pipeline=P_BYTES)
            @test z isa ZArray{Int16,1,DirectoryStore,md3type(Int16, 1, P_BYTES)}
            @test z[:] == Int16[1, 2, 3, 4]
            @test z[:] == zopen(store; path="1d.contiguous.raw.i2")[:]

            zg = zopen(Int16, Val(1), store, "1d.contiguous.gzip.i2"; pipeline=P_GZIP)
            @test zg isa ZArray{Int16,1,DirectoryStore,md3type(Int16, 1, P_GZIP)}
            @test zg[:] == Int16[1, 2, 3, 4]
            @test zg.metadata.pipeline.bytes_bytes[1] == GzipV3Codec(5)

            zb = zopen(Int16, Val(2), store, "2d.contiguous.i2"; pipeline=P_BLOSC)
            @test zb isa ZArray{Int16,2,DirectoryStore,md3type(Int16, 2, P_BLOSC)}
            @test zb[:, :] == Int16[1 3; 2 4]
            @test zb[:, :] == zopen(store; path="2d.contiguous.i2")[:, :]

            zbe = zopen(Float32, Val(1), store, "1d.contiguous.f4.be"; pipeline=P_BLOSC)
            @test zbe.metadata.pipeline.array_bytes == BytesCodec(:big)
            @test zbe[:] == Float32[-1000.5, 0.0, 1000.5, 0.0]

            PT3 = V3P{Tuple{TransposeCodec{3}},BytesCodec,Tuple{BloscV3Codec}}
            zt = zopen(Int16, Val(3), store, "3d.chunked.mixed.i2.F"; pipeline=PT3)
            @test zt isa ZArray{Int16,3,DirectoryStore,md3type(Int16, 3, PT3)}
            @test zt[:, :, :] == reshape(Int16.(0:26), 3, 3, 3)
            @test zt[:, :, :] == zopen(store; path="3d.chunked.mixed.i2.F")[:, :, :]

            # Sharded fixtures cannot be opened on the typed path.
            @test_throws ArgumentError zopen(Int16, Val(1), store,
                "1d.contiguous.compressed.sharded.i2"; pipeline=P_BYTES)
        end
    end

end
