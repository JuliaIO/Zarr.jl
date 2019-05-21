# ZarrNative.jl

ZarrNative is a Julia package providing an implementation of chunked, compressed, N-dimensional arrays. [Zarr](https://zarr.readthedocs.io/en/stable/) is originally a Python package. In ZarrNative we aim to implement the [zarr spec](https://zarr.readthedocs.io/en/stable/spec/v2.html).

| **Documentation**                                                               | **Build Status**                                                                                |
|:-------------------------------------------------------------------------------:|:-----------------------------------------------------------------------------------------------:|
| [![][docs-dev-img]][docs-dev-url] | [![][travis-img]][travis-url] [![][codecov-img]][codecov-url] |

## Package status

The package currently implements basic functionality for reading and writing zarr arrays. However, the package is under active development, since many compressors and backends supported by the python implementation are still missing.

## Links
https://discourse.julialang.org/t/a-julia-compatible-alternative-to-zarr/11842
https://github.com/zarr-developers/zarr/issues/284
https://zarr.readthedocs.io/en/stable/spec/v2.html


[docs-dev-img]: https://img.shields.io/badge/docs-dev-blue.svg
[docs-dev-url]: https://meggart.github.io/ZarrNative.jl/latest

[travis-img]: https://travis-ci.org/meggart/ZarrNative.jl.svg?branch=master
[travis-url]: https://travis-ci.org/meggart/ZarrNative.jl

[codecov-img]: https://codecov.io/gh/meggart/ZarrNative.jl/branch/master/graph/badge.svg
[codecov-url]: https://codecov.io/gh/meggart/ZarrNative.jl
