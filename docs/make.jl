using Documenter, Zarr

makedocs(
    modules = [Zarr],
    clean = false,
    format = Documenter.HTML(),
    sitename = "Zarr.jl",
    authors = "Fabian Gans, Martijn Visser",
    pages = [
        "Home" => "index.md",
        "Tutorial" => "tutorial.md",
        "Function Reference" => "reference.md",
        "Storage Backends" => "storage.md",
        "Accessing cloud data Examples" => "s3examples.md",
        "Operations on Zarr Arrays" => "operations.md",
        "Dealing with missing values" => "missings.md",
    ]
)

zarrpath = joinpath(@__DIR__, "data", "example.zarr")
isdir(zarrpath) && rm(zarrpath, recursive=true)

deploydocs(
    repo = "github.com/meggart/Zarr.jl.git",
)
