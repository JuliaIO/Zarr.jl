# only push coverage from one bot
get(ENV, "TRAVIS_OS_NAME", nothing)       == "linux" || exit(0)
get(ENV, "TRAVIS_JULIA_VERSION", nothing) == "1.5"   || exit(0)

using Coverage

repo_root = normpath(joinpath(@__DIR__, "..", "..", ".."))
package_dirs = ("Zarr", "ZarrCore", "ZarrZip", "ZarrBlosc", "ZarrZlib",
                "ZarrZstd", "ZarrHTTP", "ZarrGCS", "ZarrS3")

cd(repo_root) do
    coverage = reduce(vcat, process_folder(joinpath(package, "src")) for package in package_dirs)
    Codecov.submit(coverage)
end
