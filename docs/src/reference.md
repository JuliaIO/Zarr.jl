# API reference

## Arrays

```@docs
zcreate
zzeros
```

## Group hierarchy

```@autodocs
Modules = [Zarr]
Pages = ["ZGroup.jl"]
```

## Compressors

```@autodocs
Modules = [Zarr]
Pages = ["Compressors.jl"]
```

Additional compressors can be loaded via Julia's package extension mechanism.

For example, the "zstd" compressor ID can be enabled by loading CodecZstd.jl.
This uses Zstandard directly rather than using Blosc.

```jldoctest
using Zarr, CodecZstd
zarray = zzeros(UInt16, 1024, 512, compressor="zstd", path="zarr_zstd_demo")
zarray2 = zopen("zarr_zstd_demo")
```
