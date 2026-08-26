# ZarrTrimmable

A trim-safe, closed-set front end over `ZarrCore`'s statically typed
`zopen`/`zcreate` entry points, for building small standalone binaries with
`juliac --trim=safe`.

Two layers:

* **`@zarr_reader`** — declare a *closed set* of element types, ranks, codecs,
  stores and Zarr format versions. The macro generates a codec-pool struct and
  a store-pool struct that forward the `ZarrCore` compressor and storage
  interfaces with explicit `isa` chains, and emits `ZarrCore.zopen` /
  `ZarrCore.zcreate` methods over that set. Nothing dispatches on the open
  world, so nothing widens to `Any`.
* **`ArrayMeta`** — an `Any`-free algebraic-data-type mirror of the Zarr v2
  `.zarray` document (`DType`, `CompressorSpec`, `FillValue`), for reading,
  describing and re-writing array metadata without a codec package in sight.

`ZarrTrimmable` depends only on `ZarrCore` and `JSON`. Codec types enter a
reader through the macro's `codecs =` pool.

## Example

```julia
using ZarrCore, ZarrTrimmable, ZarrZstd, ZarrZlib, ZarrBlosc
using ZarrCore: NoCompressor, DirectoryStore

# top level only: the macro defines structs and consts
const READER = @zarr_reader(dtypes = (Float64, Int32), ndims = (2, 3),
                            codecs = (NoCompressor, ZarrZstd.ZstdCompressor,
                                      ZarrZlib.ZlibCompressor,
                                      ZarrBlosc.BloscCompressor),
                            stores = (DirectoryStore,), tag = R1)

function sum_all(z)               # a visitor; sees the concrete ZArray
    s = 0.0
    for x in z[:, :]
        s += Float64(x)
    end
    return s
end

total = zopen(sum_all, READER, "data/f64_2d_zstd.zarr")

z = zcreate(READER, "out.zarr", Float64, (4, 6);
            chunks = (2, 3), codec = ZarrZstd.ZstdCompressor(; level = 3))
z[:, :] = rand(4, 6)
```

The macro emits `CodecPool_R1`, `StorePool_R1`, `ReaderType_R1` and
`ZArrayUnion_R1` in the calling module.

## Zarr v3

`zarr_format` is a tuple literal drawn from `(2,)` (the default), `(3,)` and
`(2, 3)`; a bare integer is accepted too. When 3 is in the pool, `v3_codecs`
lists the `V3Codec{:bytes,:bytes}` types the reader accepts — explicitly, not
derived from `codecs`:

```julia
using ZarrCore, ZarrTrimmable, ZarrZstd, ZarrBlosc
using ZarrCore: NoCompressor, DirectoryStore, CRC32cV3Codec

const READER = @zarr_reader(dtypes = (Float64, Int32), ndims = (2, 3),
                            codecs = (NoCompressor, ZarrZstd.ZstdCompressor,
                                      ZarrBlosc.BloscCompressor),
                            stores = (DirectoryStore,),
                            zarr_format = (2, 3),
                            v3_codecs = (ZarrZstd.ZstdV3Codec,
                                         ZarrBlosc.BloscV3Codec, CRC32cV3Codec),
                            tag = R3)

zopen(sum_all, READER, "data/f64_2d_zstd.zarr")      # .zarray
zopen(sum_all, READER, "data/f64_2d_zstd_v3.zarr")   # zarr.json
```

The visitor tries `.zarray` first and `zarr.json` second, in whichever of the
two the `zarr_format` pool asks for, and throws an `ArgumentError` naming the
formats it tried when it finds neither.

Two more names are emitted for a v3 reader: `BBPool_R3`, the bytes→bytes codec
pool (the v3 counterpart of `CodecPool_R3`), and its pipeline type

```julia
const P_R3 = ZarrCore.V3Pipeline{Tuple{}, ZarrCore.BytesCodec, Vector{BBPool_R3}}
```

Because the bytes→bytes stage is a `Vector`, **any number of bytes→bytes codecs
in any order** costs nothing extra — a `[bytes, zstd, crc32c]` chain works as
well as a bare `[bytes]` one.

## Limitations

* **Top level only.** `@zarr_reader` defines `struct`s and `const`s; expanding
  it inside a function or a `@testset` body fails.
* **The visitor form is the primary one.** `zopen(reader, path)` (returning the
  array) is emitted only when `ZArrayUnion_R1` has at most 3 members; a wider
  reader gets a method that throws, because a `Union` of more than
  `Base.Compiler.MAX_TYPEUNION_LENGTH = 3` array types widens to bare `ZArray`.
  The count is `|dtypes| * |ndims| * |zarr_format|`, so a two-format reader
  reaches the limit with half as many `(dtype, ndims)` pairs.
* **Reading is v2 and/or v3; creating *through a reader* is v2 only.**
  `zcreate` methods are emitted only when a `codecs` pool was given (a v3-only
  reader may leave it out, and then gets none), and they always write
  `.zarray`. `ZarrCore`'s own `zcreate(T, store, dims...; zarr_format = 3)` is
  trim-clean and can be called next to a reader.
* **No filters** on the v2 path.
* **The v3 path takes bytes→bytes codecs only.** The array→bytes codec is
  always the plain `bytes` codec and the array→array stage is empty, so a
  stored `transpose`, `sharding_indexed` or `vlen-utf8` codec fails the open
  with an `ArgumentError`, as does a bytes→bytes codec outside `v3_codecs`.
  Chunk key encodings `default` and `v2` (any separator) work, `suffix` does
  not; the rest of the typed v3 path's limits apply too (regular chunk grid
  only, no `storage_transformers`, no `null` fill value).
* **`ZarrCore.store_read_strategy` is pinned** to `SequentialRead()` unless you
  pass `read_strategy = forward`; the concurrent chunk path is not trim-clean.
* **The `NoCompressor` write fast path is not taken** through a pool: a
  `CodecPool` is not a `NoCompressor`, so writes go through `zcompress!`.
* **`ConsolidatedStore`, `CachingStore`, `HTTPStore`, `S3Store` and `GCStore`
  must not be pool members** — the first two wrap another store and override
  metadata access, and the remote three are concurrent-read stores whose I/O is
  not trim-clean.
* **Binary cost:** ≈6 KB per extra codec, but ≈175 KB per extra
  `(dtype, ndims)` pair. Widen `codecs`/`stores` freely; widen `dtypes` ×
  `ndims` deliberately.

See the `UserGuide/trimming` page of the Zarr.jl documentation for the full
story.
