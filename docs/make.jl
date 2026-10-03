using DocumenterVitepress
using Documenter, Zarr
# Load implementation modules for Documenter.
using Zarr: ZarrCore, ZarrHTTP, ZarrGCS, ZarrS3, ZarrZip, ZarrBlosc, ZarrZlib, ZarrZstd

cp(joinpath(@__DIR__, "..", "CHANGELOG.md"), joinpath(@__DIR__, "src", "changelog.md"), force = true)
cp(joinpath(@__DIR__, "..", "CONTRIBUTING.md"), joinpath(@__DIR__, "src", "contributing.md"), force = true)

makedocs(
    modules = [Zarr, ZarrCore, ZarrHTTP, ZarrGCS, ZarrS3, ZarrZip, ZarrBlosc, ZarrZlib, ZarrZstd],
    clean = false,
    doctest = true,
    format = DocumenterVitepress.MarkdownVitepress(
        repo = "github.com/JuliaIO/Zarr.jl",
        devbranch = "main",
        devurl = "dev",
    ),
    source = "src",
    build = "build",
    sitename = "Zarr.jl",
    authors = "Fabian Gans, Martijn Visser",
    warnonly=[:missing_docs, :cross_references],
)

zarrpath = joinpath(@__DIR__, "data", "example.zarr")
isdir(zarrpath) && rm(zarrpath, recursive=true)

DocumenterVitepress.deploydocs(
    repo = "github.com/JuliaIO/Zarr.jl.git",
    target = joinpath(@__DIR__, "build"),
    branch = "gh-pages",
    devbranch = "main",
    push_preview = true,
)
