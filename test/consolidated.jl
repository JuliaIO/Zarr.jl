using CondaPkg: CondaPkg, PkgSpec
using JSON
using PythonCall

CondaPkg.add([
    PkgSpec("numpy"),
    PkgSpec("zarr"; version="3.*"),
    PkgSpec("numcodecs")
])

pyzarr = pyimport("zarr")
pystorage = pyimport("zarr.storage")
pyjson = pyimport("json")

path_v3 = joinpath(@__DIR__, "v3_python", "data.zarr")

@testset "Consolidated Metadata" begin
    @testset "round-trip Python vs Julia" begin
        # Python side
        store = pystorage.LocalStore(path_v3)
        group = pyzarr.open_consolidated(store, path="consolidated")
        python_str = pyconvert(String, pyjson.dumps(group.metadata.consolidated_metadata.to_dict()))
        python_parsed = JSON.parse(python_str)

        # Julia side
        consolidated_group = zopen(path_v3, path="consolidated", consolidated=true)
        julia_parsed = JSON.parse(JSON.json(consolidated_group.storage.cons))

        @test julia_parsed == python_parsed
    end
end