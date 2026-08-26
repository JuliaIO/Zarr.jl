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

`codec_id` is filtered out here and documented in the trimming section below;
a binding may only be spliced into one block.

```@autodocs
Modules = [ZarrCore]
Pages = ["Compressors/Compressors.jl"]
Filter = t -> t !== ZarrCore.codec_id
```

```@autodocs
Modules = [ZarrBlosc, ZarrZlib, ZarrZstd]
```

## Trimming (`ZarrTrimmable`)

Opt-in support for `juliac --trim=safe`; see [Compiling with juliac](UserGuide/trimming.md).
`ZarrTrimmable` is not re-exported by `Zarr`.

The statically typed open path lives in `ZarrCore` itself: `codec_id` and
`codec_name` identify a compressor or codec type without an instance, the
schema structs are the `Any`-free mirrors of `.zarray` and `zarr.json`, and
`pipeline_from_json` validates a stored v3 codec chain against a requested
`V3Pipeline` type.

```@docs
ZarrCore.codec_id
ZarrCore.parse_zarrjson
ZarrCore.ZarrJsonV3
ZarrCore.CodecJSON
ZarrCore.CodecConfigJSON
ZarrCore.Codecs.V3Codecs.codec_name
ZarrCore.Codecs.V3Codecs.pipeline_from_json
```

```@autodocs
Modules = [ZarrTrimmable]
```
