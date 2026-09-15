# ZarrS3.jl

Read/write Amazon S3 storage for Zarr arrays and groups. Install and load
`AWSS3` to activate the extension that implements S3 I/O.

## What it exposes

- `S3Store(bucket::String; aws=nothing)` (exported): a store backed by an S3
  bucket. `aws` accepts an AWS configuration; by default it uses AWSS3's current
  configuration.
- `ZarrS3.register!()`: registers `s3://` URLs with ZarrCore. This runs
  automatically unless ZarrCore's `RegisterAtInit` preference is disabled.
- With AWSS3 loaded, store reads, writes, deletion, and listing, plus a
  `ZarrCore.zopen` method for `AWSS3.S3Path` that preserves the path's configuration.

Registering the URL scheme alone does not enable S3 I/O: `AWSS3` must be loaded,
including when accessing S3 through the `Zarr` facade.

## Quick example

With AWS credentials/configuration set up for AWSS3, replace the bucket and
path with an existing Zarr array you can access:

```julia
using ZarrCore, ZarrS3, AWSS3

a = zopen("s3://my-bucket/data.zarr", "r")
size(a)
```

Load the appropriate compressor package (such as `ZarrBlosc`) before reading
compressed chunks. To use a custom AWS configuration, construct
`S3Store("my-bucket"; aws=config)` and pass it to `zopen` with `path="data.zarr"`.

[Main README](../README.md) · [Documentation](https://juliaio.github.io/Zarr.jl/) · [License](../LICENSE.md)
