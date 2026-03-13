using DocumenterVitepress
using Documenter, Zarr

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
    warnonly=[:missing_docs,],
    pages = [
        "User Guide" => [
            "Tutorial" => "tutorial.md",
            "Storage Backends" => "storage.md",
            "Accessing cloud data Examples" => "s3examples.md",
            "Operations on Zarr Arrays" => "operations.md",
            "Dealing with missing values" => "missings.md",
            ],
        "API Reference" => "reference.md",
    ]
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