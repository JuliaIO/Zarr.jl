# ZarrCore.jl

The core implementation of Zarr v2 and v3 arrays, groups, metadata, and chunk
I/O. Use it directly when you want to choose which compression and remote
storage dependencies to load.

## What it exposes

Exported names are available after `using ZarrCore`:

- `ZArray`, `ZGroup`, `zcreate`, `zzeros`, `zopen`, and `zgroup` for creating,
  opening, indexing, and updating arrays and groups.
- `DirectoryStore` for filesystem storage.
- `zarrcache` for array caching; `zinfo`, `storagesize`, and `storageratio` for
  inspecting arrays and their storage.

Additional public APIs use the `ZarrCore.` prefix or an explicit import:

- Stores: `AbstractStore`, `DictStore`, `CachingStore`, `ConsolidatedStore`,
  and `consolidate_metadata`.
- Filters: `VLenArrayFilter`, `VLenUTF8Filter`, `Fletcher32Filter`,
  `FixedScaleOffsetFilter`, `ShuffleFilter`, `QuantizeFilter`, and `DeltaFilter`.
- Codecs: `Codec`, `V3Codec`, `BytesCodec`, `TransposeCodec`, `ShardingCodec`,
  `CRC32cCodec`, `CRC32cV3Codec`, and `VLenUTF8V3Codec`, with codec interfaces in
  `ZarrCore.Codecs` and `ZarrCore.Codecs.V3Codecs`.
- Extension interfaces: store operations such as `subkeys`, `subdirs`, and
  `storefromstring`; `Filter` with `zencode`/`zdecode` and `register_filter`;
  `Compressor` with `zcompress`/`zuncompress` and `v2_to_v3_codecs`; and chunk key
  encoding types and registration functions.
- Type and fill-value conversion: `typestr`, `fill_value_encoding`, and
  `fill_value_decoding`.

Bare `ZarrCore` defaults to `ZarrCore.NoCompressor()`. Load
[ZarrBlosc](../ZarrBlosc), [ZarrZlib](../ZarrZlib), or [ZarrZstd](../ZarrZstd)
to use those compressors; loading ZarrBlosc also makes Blosc the default.
Remote and archive stores live in [ZarrHTTP](../ZarrHTTP), [ZarrGCS](../ZarrGCS),
[ZarrS3](../ZarrS3), and [ZarrZip](../ZarrZip).

Backend and compressor registration runs automatically unless ZarrCore's
`RegisterAtInit` preference is disabled. In that case, call the relevant
package's `register!()` before opening data that relies on its registry entries.

[Main README](../README.md) · [Documentation](https://juliaio.github.io/Zarr.jl/) · [License](../LICENSE.md)
