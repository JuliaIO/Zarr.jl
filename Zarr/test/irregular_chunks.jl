using DiskArrays: DiskArrays, GridChunks, IrregularChunks, RegularChunks

@testset "Zarr v3 rectilinear chunk grids" begin
    chunks = GridChunks(
        RegularChunks(2, 0, 5),
        IrregularChunks(; chunksizes=[3, 4, 5, 6, 2]),
    )

    @testset "read, write, and exact stored extents" begin
        z = zcreate(Int32, 5, 20;
            zarr_format=3,
            chunks,
            compressor=Zarr.NoCompressor(),
        )
        data = reshape(Int32.(1:100), 5, 20)
        z[:, :] = data

        @test DiskArrays.eachchunk(z) == chunks
        @test z[:, :] == data
        @test z[2:4, 2:10] == data[2:4, 2:10]
        @test z[1:5, 6:18] == data[1:5, 6:18]

        z[2:5, 6:15] .= Int32(-7)
        data[2:5, 6:15] .= Int32(-7)
        @test z[:, :] == data

        stored_lengths = sort([
            length(bytes) for (key, bytes) in z.storage.a if startswith(key, "c/")
        ])
        expected_lengths = sort(vec([
            sizeof(Int32) * rows * columns
            for rows in (2, 2, 1), columns in (3, 4, 5, 6, 2)
        ]))
        @test stored_lengths == expected_lengths
    end

    @testset "missing chunks and zero initialization" begin
        z = zcreate(Int16, 5, 20; zarr_format=3, chunks, fill_value=Int16(0))
        z[2:4, 3:9] .= Int16(12)
        expected = zeros(Int16, 5, 20)
        expected[2:4, 3:9] .= Int16(12)
        @test z[:, :] == expected

        zz = zzeros(Int16, 5, 20; zarr_format=3, chunks)
        @test zz[:, :] == zeros(Int16, 5, 20)

        with_missing = zcreate(Int16, 5, 20;
            zarr_format=3,
            chunks,
            fill_value=Int16(-1),
            fill_as_missing=true,
        )
        expected_missing = Matrix{Union{Missing,Int16}}(missing, 5, 20)
        expected_missing[2:4, 3:9] .= Int16(12)
        expected_missing[3, 5] = missing
        with_missing[2:4, 3:9] = expected_missing[2:4, 3:9]
        @test all(isequal.(with_missing[:, :], expected_missing))
    end

    @testset "metadata round-trip and run-length encoding" begin
        mktempdir() do dir
            repeated = GridChunks(
                IrregularChunks(; chunksizes=[2, 2, 3, 3]),
                RegularChunks(4, 0, 8),
            )
            z = zcreate(Int16, 10, 8;
                zarr_format=3,
                chunks=repeated,
                compressor=Zarr.NoCompressor(),
                path=dir,
            )
            data = reshape(Int16.(1:80), 10, 8)
            z[:, :] = data

            metadata = JSON.parsefile(joinpath(dir, "zarr.json"))
            chunk_grid = metadata["chunk_grid"]
            @test chunk_grid["name"] == "rectilinear"
            @test chunk_grid["configuration"]["kind"] == "inline"
            @test chunk_grid["configuration"]["chunk_shapes"] ==
                Any[4, Any[Any[2, 2], Any[3, 2]]]

            reopened = zopen(dir)
            @test DiskArrays.eachchunk(reopened) == repeated
            @test reopened[:, :] == data
        end
    end

    @testset "metadata validation and boundary clipping" begin
        z = zcreate(Int32, 5, 20;
            zarr_format=3,
            chunks,
            compressor=Zarr.NoCompressor(),
        )
        metadata = JSON.parse(JSON.json(z.metadata); dicttype=Dict{String,Any})
        metadata["chunk_grid"]["configuration"]["chunk_shapes"][1] = Any[3, 4, 5, 10]
        parsed = ZarrCore.Metadata(metadata, false)
        parsed_axis = parsed.chunks.chunks[2]
        @test parsed_axis isa IrregularChunks
        @test diff(parsed_axis.offsets) == [3, 4, 5, 8]

        too_short = deepcopy(metadata)
        too_short["chunk_grid"]["configuration"]["chunk_shapes"][1] = Any[3, 4]
        @test_throws DimensionMismatch ZarrCore.Metadata(too_short, false)

        bad_rank = GridChunks(IrregularChunks(; chunksizes=[2, 3]))
        @test_throws DimensionMismatch zcreate(Int, 5, 20; zarr_format=3, chunks=bad_rank)

        bad_shape = GridChunks(
            RegularChunks(2, 0, 6),
            IrregularChunks(; chunksizes=[3, 4, 5, 6, 2]),
        )
        @test_throws DimensionMismatch zcreate(Int, 5, 20; zarr_format=3, chunks=bad_shape)

        offset_grid = GridChunks(
            RegularChunks(2, 1, 5),
            IrregularChunks(; chunksizes=[3, 4, 5, 6, 2]),
        )
        @test_throws ArgumentError zcreate(Int, 5, 20; zarr_format=3, chunks=offset_grid)
        @test_throws ArgumentError zcreate(Int, 5, 20; zarr_format=2, chunks=chunks)
    end

    @testset "resize rejection does not mutate the array" begin
        z = zcreate(Int, 5, 20; zarr_format=3, chunks)
        @test_throws ArgumentError resize!(z, 6, 20)
        @test size(z) == (5, 20)
        @test DiskArrays.eachchunk(z) == chunks
    end
end
