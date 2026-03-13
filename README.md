# Zarr.jl

<div align="center">

[![][docs-stable-img]][docs-stable-url]
[![][docs-dev-img]][docs-dev-url]
[![][ci-img]][ci-url]
[![][codecov-img]][codecov-url]
[![][julia-img]][julia-url]

</div>

Zarr is a Julia package providing an implementation of chunked, compressed, N-dimensional arrays. [Zarr](https://zarr.readthedocs.io/en/stable/) is originally a Python package. In Zarr we aim to implement the [zarr spec](https://zarr.readthedocs.io/en/stable/spec/v2.html).

## Package status

The package currently implements basic functionality for reading and writing zarr arrays. However, the package is under active development, since many compressors and backends supported by the python implementation are still missing.

## Quick start

````julia
using Zarr
z1 = zcreate(Int, 10000,10000,path = "data/example.zarr",chunks=(1000, 1000))
z1[:] .= 42
z1[:,1] = 1:10000
z1[1,:] = 1:10000

z2 = zopen("data/example.zarr")
z2[1:10,1:10]
````

## Links
- https://discourse.julialang.org/t/a-julia-compatible-alternative-to-zarr/11842
- https://github.com/zarr-developers/zarr/issues/284
- https://zarr.readthedocs.io/en/stable/spec/v2.html

[docs-stable-img]: https://img.shields.io/badge/documentation-stable%20release-blue?style=round-square
[docs-stable-url]: https://juliaio.github.io/Zarr.jl/stable/

[docs-dev-img]: https://img.shields.io/badge/documentation-in%20development-orange?style=round-square
[docs-dev-url]: https://juliaio.github.io/Zarr.jl/dev/

[codecov-img]: https://codecov.io/gh/meggart/Zarr.jl/branch/master/graph/badge.svg
[codecov-url]: https://codecov.io/gh/meggart/Zarr.jl

[ci-img]: https://github.com/JuliaIO/Zarr.jl/actions/workflows/CI.yml/badge.svg?style=round-square
[ci-url]: https://github.com/JuliaIO/Zarr.jl/actions/workflows/CI.yml

[julia-img]: https://img.shields.io/badge/julia-v1.10+-blue.svg?style=round-square
[julia-url]: https://julialang.org/
