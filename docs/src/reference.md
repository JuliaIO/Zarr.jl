# API reference

## Arrays

```@docs
zcreate
zzeros
```

## Group hierarchy

```@autodocs
Modules = [ZarrCore]
Pages = ["ZGroup.jl"]
```

## Compressors

The compressor interface and `NoCompressor` live in `ZarrCore`; every concrete
compressor lives in its own subpackage alongside the matching Zarr v3 codec.

```@autodocs
Modules = [ZarrCore]
Pages = ["Compressors/Compressors.jl"]
```

```@autodocs
Modules = [ZarrBlosc, ZarrZlib, ZarrZstd]
Filter = item -> !(item in (ZarrBlosc.register!, ZarrZlib.register!, ZarrZstd.register!))
```

## Extension registration

Each extension package exposes its own qualified registration function:

```@docs
ZarrCore.should_register_at_init
ZarrBlosc.register!
ZarrZlib.register!
ZarrZstd.register!
ZarrHTTP.register!
ZarrGCS.register!
ZarrS3.register!
ZarrZip.register!
```
