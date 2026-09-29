@testset "Zarr v3 dimension names" begin
    path = mktempdir()
    z = zcreate(Float32, 5, 7, 2; path, zarr_format=3,
        chunks=(4, 4, 1), dimension_names=(:x, :y, :frequency))
    z[:, :, :] = reshape(Float32.(1:70), 5, 7, 2)

    @test z.metadata.dimension_names == (:x, :y, :frequency)
    @test JSON.parsefile(joinpath(path, "zarr.json"))["dimension_names"] ==
        ["frequency", "y", "x"]
    @test zopen(path).metadata.dimension_names == z.metadata.dimension_names
    @test zopen(path)[:, :, :] == z[:, :, :]

    resize!(z, 5, 7, 3)
    @test zopen(path).metadata.dimension_names == z.metadata.dimension_names

    @test_throws DimensionMismatch zcreate(Float32, 2, 3; zarr_format=3,
        dimension_names=(:x,))
    @test_throws ArgumentError zcreate(Float32, 2, 3; zarr_format=3,
        dimension_names=("x", :y))
    @test_throws ArgumentError zcreate(Float32, 2, 3; zarr_format=3,
        dimension_names=["x", "y"])
    @test zcreate(Float32, 2, 3; zarr_format=3,
        dimension_names=(:x, nothing)).metadata.dimension_names == (:x, nothing)
end
