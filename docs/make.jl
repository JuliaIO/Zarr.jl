using DocumenterVitepress
using Documenter, Zarr

cp(joinpath(@__DIR__, "..", "CHANGELOG.md"), joinpath(@__DIR__, "src", "changelog.md"), force = true)
cp(joinpath(@__DIR__, "..", "CONTRIBUTING.md"), joinpath(@__DIR__, "src", "contributing.md"), force = true)

makedocs(
    modules = [Zarr],
    clean = false,
    doctest = true,
    format = DocumenterVitepress.MarkdownVitepress(
        repo = "github.com/JuliaIO/Zarr.jl",
        devbranch = "master",
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
    devbranch = "master",
    push_preview = true,
)