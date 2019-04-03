using Documenter, ZarrNative

makedocs(
    modules = [ZarrNative],
    clean   = false,
    format   = Documenter.HTML(),
    sitename = "ZarrNative.jl",
    authors = "Fabian Gans, Martijn Visser",
    pages    = Any[ # Compat: `Any` for 0.4 compat
        "Home" => "index.md",
        "Tutorial" => "tutorial.md",
        "Function Reference" => "reference.md",
    ]
)

rm("data/example.zarr",recursive=true)

deploydocs(
    repo = "github.com/meggart/ZarrNative.jl.git",
)
