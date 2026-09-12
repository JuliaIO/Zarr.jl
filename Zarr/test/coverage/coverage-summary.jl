####
#### Coverage summary, printed as "(percentage) covered".
####
#### Useful for CI environments that just want a summary (eg a Gitlab setup).
####

using Coverage
repo_root = normpath(joinpath(@__DIR__, "..", "..", ".."))
package_dirs = ("Zarr", "ZarrCore", "ZarrZip", "ZarrBlosc", "ZarrZlib",
                "ZarrZstd", "ZarrHTTP", "ZarrGCS", "ZarrS3")

cd(repo_root) do
    coverage = reduce(vcat, process_folder(joinpath(package, "src")) for package in package_dirs)
    covered_lines, total_lines = get_summary(coverage)
    percentage = covered_lines / total_lines * 100
    println("($(percentage)%) covered")
end
