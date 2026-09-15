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

Read a few time coordinates from the public MUR sea-surface-temperature
dataset used by this repository's tests. No AWS credentials are needed:

```julia
using ZarrCore, ZarrS3, ZarrBlosc, AWSS3

config = AWSS3.AWS.AWSConfig(creds=nothing, region="us-west-2")
path = AWSS3.S3Path("s3://mur-sst/zarr-v1", config=config)
g = zopen(path, "r")
g["time"][1:5] # [0, 1, 2, 3, 4]
```

This example requires internet access and loads `ZarrBlosc` to decode compressed
chunks. For private data, use your authenticated AWS configuration. Alternatively, construct
`S3Store("my-bucket"; aws=config)` and pass it to `zopen` with `path="data.zarr"`.

[Main README](../README.md) · [Documentation](https://juliaio.github.io/Zarr.jl/) · [License](../LICENSE.md)
