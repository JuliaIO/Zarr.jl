# Optional integration tests against public Zarr v3 sharded datasets over HTTP.
#
# These tests make real network requests and are skipped by default.
# Set ZARR_TEST_REMOTE=true to enable them:
#
#   ZARR_TEST_REMOTE=true julia --project=test -e 'include("test/http_sharded.jl")'
#
# Datasets tested:
#   TESSERA 2024 geospatial embedding store (https://dl2.geotessera.org)
#   - Arrays: utm29/embeddings (3D Int8), utm29/rgb (3D UInt8), utm29/scales (2D Float32)
#   - Codec: sharding_indexed → bytes + blosc(zstd/bitshuffle) + crc32c index
#
#   OME-NGFF Challenge datasets (https://github.com/ome/ome2024-ngff-challenge)
#   - IDR 6001240: 4D UInt16 confocal, single outer chunk, inner (1,1,275,271)
#   - Flamingo Platynereis H2B: 3D UInt16 light-sheet, shards 512³, inner 128³

if get(ENV, "ZARR_TEST_REMOTE", "false") != "true"
    @info "Skipping remote HTTP sharded tests (set ZARR_TEST_REMOTE=true to enable)"
else

const TESSERA_BASE = "https://dl2.geotessera.org/zarr/v1/2024.zarr"

@testset "Remote HTTP sharded arrays (TESSERA)" begin

    @testset "utm29/embeddings" begin
        url = "$TESSERA_BASE/utm29/embeddings"
        z = zopen(url)

        # Metadata
        @test eltype(z) == Int8
        @test ndims(z) == 3
        # Zarr spec shape is [northing, easting, band] = [1355776, 66560, 128]
        # Julia reverses to column-major: (128, 66560, 1355776)
        @test size(z) == (128, 66560, 1355776)
        # Outer shard shape [256, 256, 128] → Julia (128, 256, 256)
        @test z.metadata.chunks == (128, 256, 256)

        # Verify the codec is sharding_indexed with the expected inner chunk shape
        pipeline = z.metadata.pipeline
        sharding = pipeline.array_bytes
        @test sharding isa Zarr.Codecs.V3Codecs.ShardingCodec
        # Inner chunk [4, 4, 128] → Julia (128, 4, 4)
        @test sharding.chunk_shape == (128, 4, 4)
        @test sharding.index_location == :end

        # Fill-value region: first pixel is likely ocean (all zeros)
        first_chunk = z[1:128, 1:4, 1:4]
        @test size(first_chunk) == (128, 4, 4)
        @test eltype(first_chunk) == Int8

        # Mid-array region over land should have non-zero embedding values
        mid = z[1:128, 33000:33003, 677000:677003]
        @test size(mid) == (128, 4, 4)
        @test any(!=(0), mid)
        @test minimum(mid) >= -128
        @test maximum(mid) <= 127
    end

    @testset "utm29/rgb" begin
        url = "$TESSERA_BASE/utm29/rgb"
        z = zopen(url)

        @test eltype(z) == UInt8
        @test ndims(z) == 3
        # Spec shape [northing, easting, band=4] → Julia (4, 66560, 1355776)
        @test size(z) == (4, 66560, 1355776)

        pipeline = z.metadata.pipeline
        @test pipeline.array_bytes isa Zarr.Codecs.V3Codecs.ShardingCodec

        rgb = z[1:3, 33000:33003, 677000:677003]
        @test size(rgb) == (3, 4, 4)
        @test eltype(rgb) == UInt8
        @test any(!=(0x00), rgb)
    end

    @testset "utm29/scales" begin
        url = "$TESSERA_BASE/utm29/scales"
        z = zopen(url)

        # scales is a 2D Float32 array [northing, easting] → Julia (66560, 1355776)
        @test eltype(z) == Float32
        @test ndims(z) == 2
        @test z.metadata.pipeline.array_bytes isa Zarr.Codecs.V3Codecs.ShardingCodec

        slice = z[33000:33003, 677000:677003]
        @test size(slice) == (4, 4)
        @test any(isfinite, slice)
    end

    @testset "multiple UTM zones readable" begin
        for zone in ("utm29", "utm30", "utm31")
            z = zopen("$TESSERA_BASE/$zone/embeddings")
            @test eltype(z) == Int8
            @test ndims(z) == 3
            @test z.metadata.pipeline.array_bytes isa Zarr.Codecs.V3Codecs.ShardingCodec
        end
    end

end # @testset "Remote HTTP sharded arrays (TESSERA)"

# OME-NGFF Challenge datasets (https://github.com/ome/ome2024-ngff-challenge)
# All use sharding_indexed → bytes + blosc(zstd) inner + bytes + crc32c index

const IDR_BASE  = "https://livingobjects.ebi.ac.uk/idr/share/ome2024-ngff-challenge"
const FLAMINGO_BASE = "https://radosgw.public.os.wwu.de/n4bi-goe"

@testset "Remote HTTP sharded arrays (OME-NGFF Challenge)" begin

    @testset "IDR 6001240 — 4D UInt16 confocal" begin
        # Shape [c,z,y,x] = [2,236,275,271]; single outer chunk equals full shape;
        # inner chunk [1,1,275,271] → Julia (271,275,1,1)
        z = zopen("$IDR_BASE/0.0.5/6001240.zarr/0")

        @test eltype(z) == UInt16
        @test ndims(z) == 4
        @test size(z) == (271, 275, 236, 2)

        sharding = z.metadata.pipeline.array_bytes
        @test sharding isa Zarr.Codecs.V3Codecs.ShardingCodec
        @test sharding.chunk_shape == (271, 275, 1, 1)

        slice = z[1:10, 1:10, 1:5, 1:2]
        @test size(slice) == (10, 10, 5, 2)
        @test eltype(slice) == UInt16
    end

    @testset "Flamingo Platynereis H2B — 3D UInt16 light-sheet" begin
        # Shape [z,y,x] = [192,1024,1024]; outer shard 512³; inner chunk 128³
        z = zopen("$FLAMINGO_BASE/Platynereis-H2B-TL.ome.zarr/setup0/timepoint0/s0")

        @test eltype(z) == UInt16
        @test ndims(z) == 3
        @test size(z) == (1024, 1024, 192)
        @test z.metadata.chunks == (512, 512, 512)

        sharding = z.metadata.pipeline.array_bytes
        @test sharding isa Zarr.Codecs.V3Codecs.ShardingCodec
        @test sharding.chunk_shape == (128, 128, 128)

        slice = z[1:128, 1:128, 1:128]
        @test size(slice) == (128, 128, 128)
        @test eltype(slice) == UInt16
        @test any(!=(0x0000), slice)
    end

end # @testset "Remote HTTP sharded arrays (OME-NGFF Challenge)"

end # if ZARR_TEST_REMOTE
