# Get Started

`Zarr.jl` is a Julia package for working with chunked, compressed, N-dimensional arrays, compatible with the [Zarr](https://zarr.dev) format used across [Python](https://zarr.readthedocs.io/en/stable/), [Rust](https://docs.rs/zarrs/latest/zarrs/), and other ecosystems.

## Installation

Install [`Julia v1.10`](https://julialang.org/downloads/) or above. `Zarr.jl` is available through the Julia package manager.
You can enter it by pressing `]` in the REPL and then typing `add Zarr`:

```julia
(@v1.x) pkg> add Zarr
```

alternatively, you can also do:

```julia
import Pkg
Pkg.add("Zarr")
```

It is recommended to check the version of `Zarr.jl` you have installed with the `status` command:
```julia
(@v1.x) pkg> status Zarr
```

or:

```@example
import Pkg
Pkg.status("Zarr")
```

Where not shown explicitly, this documentation assumes `using Zarr` has been evaluated in your session:

```julia
using Zarr
```

## Creating Arrays

Use `zcreate` for new arrays, `zzeros` for zero-initialized ones, or `ZArray` to wrap an existing Julia array:

:::tabs

== Version 3

```@example 
using Zarr

z = zcreate(Float32, 1000, 1000;
    chunks=(100, 100),
    fill_value=Float32(0),
    zarr_format=3,
    path="example_v3.zarr")
```

== Version 2

```@example
using Zarr

# v2 array on disk (default)
z = zcreate(Float32, 1000, 1000;
    chunks=(100, 100),
    fill_value=Float32(0),
    path="example_v2.zarr")

```

:::

you can also wrap an existing Julia array:

```@example julia-array
using Zarr
z = ZArray(rand(Float64, 100, 100))
```

```@example julia-array
zinfo(z)
```

## Reading and Writing
```@example rw
using Zarr

z = zcreate(Float32, 1000, 1000;
    chunks=(100, 100),
    fill_value=Float32(0),
    path="example.zarr")

z[:, :] = rand(Float32, 1000, 1000)  # write entire array
z[1, :] = 1:1000                     # write a row

subset = z[1:3, 1:10]                # read a subregion
```

## Opening Existing Arrays

Zarr automatically detects v2 or v3 format on open:
```@example rw
z = zopen("example.zarr")

println(size(z))    # (1000, 1000)
println(eltype(z))  # Float32
```

::: warning

`zopen` throws an `ArgumentError` if the path does not exist. Make sure the path points to a valid Zarr store.

:::

## Missing Values

Use `fill_value` and `fill_as_missing` together to handle missing data:
```@example missing
using Zarr

z = zcreate(Int64, 10, 10;
    chunks=(5, 2),
    fill_value=-1,
    fill_as_missing=true)

z[:, 1] = 1:10          # write a column
z[:, 2] .= missing      # mark a column as missing

println(eltype(z))               # Union{Int64, Missing}
println(all(ismissing, z[:, 2])) # true
```

Re-open with or without missing support:

```@example missing
# treat fill_value as missing
z = zopen("example.zarr", fill_as_missing=true)
```

or treat `fill_value` as a regular value

```@example missing
z = zopen("example.zarr")
```

## Compression

Zarr uses Blosc (lz4, level 5) by default. Several compressors are available:

:::tabs

```@example compression
using Zarr
```

== Blosc (zstd)

```@example compression

z = zcreate(Int32, 1000, 1000;
    chunks=(100, 100),
    compressor=Zarr.BloscCompressor(cname="zstd", clevel=3, shuffle=true),
    fill_value=Int32(0),
    path="blosc.zarr")

z[:,:] = Int32(1):Int32(1000*1000)
storageratio(z)
```

== Zstd

```@example compression

z = zcreate(Int32, 1000, 1000;
    chunks=(100, 100),
    compressor=Zarr.ZstdCompressor(),
    fill_value=Int32(0),
    path="zstd.zarr")

z[:,:] = Int32(1):Int32(1000*1000)
storageratio(z)
```

== No compression

```@example compression

z = zcreate(Float32, 1000, 1000;
    chunks=(100, 100),
    compressor=Zarr.NoCompressor(),
    fill_value=Float32(0),
    path="raw.zarr")

z[:,:] = Int32(1):Int32(1000*1000)
storageratio(z)
```

:::

## Resizing and Appending

```@example resize
using Zarr

z = zzeros(Int64, 10, 10; chunks=(5, 2), fill_value=-1)
```

grow first dimension

```@example resize
resize!(z, 20, 10)
z
```

appends columns

```@example resize
append!(z, rand(Int64, 20, 5))
z
```

append a row

```@example resize
append!(z, rand(Int64, 15), dims=1)
z
```

## Groups

Zarr allows you to create hierarchical groups, similar to directories:
```@example groups
using Zarr

store = Zarr.DirectoryStore("experiment.zarr")
g = zgroup(store, "", Zarr.ZarrFormat(3))

zcreate(Float64, g, "temperature", 100, 100; chunks=(50, 50), fill_value=0.0)
zcreate(Float64, g, "precipitation", 100, 100; chunks=(50, 50), fill_value=0.0)
g
```

Navigate into a group to access its arrays:
```@example groups
temp = g["temperature"]
println(size(temp))  # (100, 100)
```

## Storage Backends

Zarr supports several storage backends out of the box:

:::tabs

== Local disk
```julia
z = zopen("example.zarr")
```

== In-memory
```julia
z = zzeros(Float32, 100, 100)
```

== S3
```julia
z = zopen("s3://my-bucket/data.zarr")
```

== GCS
```julia
# via native GCS
z = zopen("gs://my-bucket/data.zarr")

# via HTTPS
z = zopen("https://storage.googleapis.com/my-bucket/data.zarr")
```

== HTTP
```julia
z = zopen("http://my-server/data.zarr")
```

== Zip
```julia
ds = Zarr.ZipStore("archive.zip")
z  = zopen(ds)
```

:::

See [Storage Backends](./UserGuide/storage) for full details on credentials and configuration.

::: tip

Ready for more? Head to the [User Guide](./tutorials/tutorial) for a deeper dive.

:::