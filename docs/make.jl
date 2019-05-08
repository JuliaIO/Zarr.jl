using Documenter, ZarrNative

makedocs(
    modules = [ZarrNative],
    clean = false,
    format = Documenter.HTML(),
    sitename = "ZarrNative.jl",
    authors = "Fabian Gans, Martijn Visser",
    pages = [
        "Home" => "index.md",
        "Tutorial" => "tutorial.md",
        "Function Reference" => "reference.md",
        "Storage Backends" => "storage.md",
    ]
)

zarrpath = joinpath(@__DIR__, "data", "example.zarr")
isdir(zarrpath) && rm(zarrpath, recursive=true)

deploydocs(
    repo = "github.com/meggart/ZarrNative.jl.git",
)
