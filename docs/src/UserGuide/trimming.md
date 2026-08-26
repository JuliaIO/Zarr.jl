# Compiling with juliac (`ZarrTrimmable`)

`ZarrTrimmable` is an opt-in subpackage for programs that are compiled with
`juliac --trim=safe`. Trimming rejects every call that cannot be resolved
statically, and the dynamic `zopen(path)` cannot be: the element type, rank
and compressor of an array are only known once `.zarray` has been read.

`ZarrCore` already ships a statically typed open,
`zopen(T, Val(N), store, path; compressor = C)` for Zarr v2 and
`zopen(T, Val(N), store, path; pipeline = P)` (with `P<:V3Pipeline` naming the
whole codec chain) for Zarr v3, which is trim-clean when
every type is a literal at the call site. `ZarrTrimmable` adds a
*closed-set front end* over both: you declare the element types, ranks,
compressors, stores and Zarr format versions your program will ever meet, and
a macro generates a reader that dispatches into one of those combinations
without any open-world call.

`ZarrTrimmable` is **not** re-exported by `Zarr`. Load it next to `ZarrCore`
and the codec packages you need:

```julia
using ZarrCore, ZarrZstd, ZarrBlosc, ZarrTrimmable
```

## Typed Zarr v3 open

A `zarr.json` array is opened by naming the element type, the rank and the
whole codec pipeline:

```julia
using ZarrCore, ZarrZstd
using ZarrCore: DirectoryStore, BytesCodec, V3Pipeline
using ZarrZstd: ZstdV3Codec

z = zopen(Float64, Val(2), DirectoryStore("data3.zarr"), "";
          pipeline = V3Pipeline{Tuple{}, BytesCodec, Tuple{ZstdV3Codec}})
a = z[:, :]::Matrix{Float64}
```

`compressor` and `pipeline` are the two codec keywords of the same `zopen`
method and exactly one of them must be given: `compressor = C` selects the
Zarr v2 path (`.zarray`), `pipeline = P` the Zarr v3 one (`zarr.json`).
Neither has a default, and giving both or neither is an `ArgumentError`.
The result is a fully concrete
`ZArray{T,N,typeof(store),MetadataV3{T,N,P,ChunkKeyEncoding}}`; the stored
metadata is *validated* against `T`, `N` and `P` rather than used to infer
them, so nothing widens to `Any`.

### Spelling the pipeline type

`V3Pipeline{AA, AB, BB}` mirrors the three stages of the v3 codec chain in
store order: `AA` is a `Tuple` of array→array codecs, `AB` the single
array→bytes codec, `BB` the bytes→bytes codecs.

| stored `codecs` | pipeline type |
|---|---|
| `[bytes]` | `V3Pipeline{Tuple{}, BytesCodec, Tuple{}}` |
| `[bytes, zstd]` | `V3Pipeline{Tuple{}, BytesCodec, Tuple{ZstdV3Codec}}` |
| `[bytes, blosc, crc32c]` | `V3Pipeline{Tuple{}, BytesCodec, Tuple{BloscV3Codec, CRC32cV3Codec}}` |
| `[transpose, bytes, gzip]` | `V3Pipeline{Tuple{TransposeCodec{2}}, BytesCodec, Tuple{GzipV3Codec}}` |

`TransposeCodec{N}` carries the rank, which must equal the array's `N`. If
`BB` is written as an `AbstractVector{C}` instead of a `Tuple`, *any* number of
trailing bytes→bytes codecs is accepted and each is parsed as a `C` — that is
how `@zarr_reader` builds its codec pool (see below). A count or name mismatch
between the store and the requested pipeline is an `ArgumentError`.

A codec type can appear in a pipeline once it defines
`V3Codecs.codec_name(::Type{C})` (its spec name) and
`V3Codecs.getCodec(::Type{C}, ::ZarrCore.CodecJSON, ctx)`, next to the usual
`codec_encode`, `codec_decode` and `JSON.lower`. `ZarrCore` defines them for
`bytes`, `transpose`, `crc32c`; `ZarrZstd`, `ZarrZlib` and `ZarrBlosc` for
`zstd`, `gzip` and `blosc`.

### Creating v3 arrays

`zcreate(T, store, dims...; zarr_format = 3, ...)` is trim-clean too, as long
as `zarr_format = 3` is a literal at the call site (the keyword wrappers are
`@inline`d into a positional barrier so that the format reaches it as a
compile-time constant). It writes a single `zarr.json` with `attributes`
included. Writing a *sharded* v3 array from a compiled program is not
trim-clean.

### What the typed v3 path accepts

| aspect | what the typed path takes |
|---|---|
| chunk grid | `regular` only |
| chunk key encoding | `default` and `v2`, with any separator; `suffix` rejected |
| data types | every entry of `typemap3` (integers, floats, `Bool`, `complex64`, `complex128`); the object-spelled `fixed_length_utf32` is rejected |
| codecs | `bytes`, `transpose` (integer `order` only), `crc32c`, `zstd`, `gzip`, `blosc`; `sharding_indexed` and `vlen-utf8` rejected |
| fill value | number, `true`/`false`, and the strings `"NaN"`/`"Infinity"`/`"-Infinity"`; `null` and complex/array fill values rejected |
| `storage_transformers` | must be absent or empty |
| other | `attributes` and `fill_as_missing` supported; `dimension_names` is ignored |

Everything in the rejected column throws an `ArgumentError` (or a JSON parse
error, for an object-spelled `data_type`) naming what it found. The dynamic
`zopen(path)` still handles all of it.

### Compiled program sizes

Julia 1.12.6, `juliac --trim=safe`, x86-64 Linux; every program links 0
verifier errors.

| program | what it does | size | compile |
|---|---|---|---|
| `read_v3_typed` | typed v3 opens, bytes-only and bytes+zstd chains | 5,110,120 B | 11 s |
| `write_v3` | `zcreate(...; zarr_format = 3)` + chunk writes | 4,570,784 B | 11 s |
| `roundtrip_v3` | `zcreate` then typed open of the same store, bytes and zstd | 7,349,936 B | 14 s |
| `trimmable_v3` | `@zarr_reader` over 12 v2 + 12 v3 stores | 6,520,120 B | 12 s |
| `read_typed_zstd` (v2 baseline) | typed v2 open | 4,257,520 B | |
| `write_zstd_v4` (v2 baseline) | v2 `zcreate` | 4,274,944 B | |
| `trimmable_reader` (v2 baseline) | `@zarr_reader` over 12 v2 stores | 5,032,048 B | |

## `@zarr_reader`

```julia
using ZarrCore, ZarrZstd, ZarrBlosc, ZarrTrimmable
using ZarrCore: NoCompressor, DirectoryStore
using ZarrZstd: ZstdCompressor
using ZarrBlosc: BloscCompressor

const READER = @zarr_reader(dtypes = (Float64, Int32), ndims = (2, 3),
                            codecs = (NoCompressor, ZstdCompressor, BloscCompressor),
                            stores = (DirectoryStore,), tag = R1)

# visitor form: `f` receives the fully concrete ZArray
total = zopen(z -> sum(z[:, :]), READER, "data/f64_2d_zstd.zarr")

# create through the same closed set
z = zcreate(READER, "out.zarr", Float64, (4, 6);
            chunks = (2, 3), codec = ZstdCompressor(; level = 3))
z[:, :] = rand(4, 6)
```

The macro must be expanded at the top level of a module or script (it
defines `struct`s and `const`s). With `tag = R1` it emits `CodecPool_R1`,
`StorePool_R1`, `ReaderType_R1` and `ZArrayUnion_R1` in the calling module
and returns the reader value.

The reader adds methods to the ordinary `ZarrCore` verbs:

| call | availability |
|---|---|
| `zopen(f, reader, store_or_path[, sub])` | always; `f` sees the concrete `ZArray` |
| `zopen(reader, store_or_path[, sub])` | only when `ZArrayUnion_R1` has at most 3 members, i.e. `length(dtypes) * length(ndims) * length(zarr_format) <= 3`; result must be asserted `::ZArrayUnion_R1` at the call site. Wider readers get a method that throws. |
| `zcreate(reader, store_or_path, T, dims; codec, chunks, fill_value, sub)` | one method per rank in `ndims`, and only when a `codecs` pool was given (creation is Zarr v2 only) |

Only `length(dtypes) * length(ndims)` `ZArray` specialisations per format are
ever instantiated. Codecs and stores are *values* inside the generated pool
structs (a `Union`-typed field), and every interface function is forwarded
through an explicit `isa` chain, so a codec or store pool wider than the
compiler's union-splitting limit still resolves statically.

## Reading Zarr v3: `zarr_format` and `v3_codecs`

`zarr_format` is a tuple literal drawn from `(2,)` (the default), `(3,)` and
`(2, 3)`; a bare integer (`zarr_format = 3`) is accepted too. When 3 is in the
pool, `v3_codecs` must be a non-empty tuple of `V3Codec{:bytes,:bytes}` types.
They are listed explicitly rather than derived from `codecs`, so no
v2-compressor-to-v3-codec mapping is involved:

```julia
using ZarrCore, ZarrZstd, ZarrBlosc, ZarrTrimmable
using ZarrCore: NoCompressor, DirectoryStore, CRC32cV3Codec
using ZarrZstd: ZstdCompressor, ZstdV3Codec
using ZarrBlosc: BloscCompressor, BloscV3Codec

const READER = @zarr_reader(dtypes = (Float64, Int32), ndims = (2, 3),
                            codecs = (NoCompressor, ZstdCompressor, BloscCompressor),
                            stores = (DirectoryStore,),
                            zarr_format = (2, 3),
                            v3_codecs = (ZstdV3Codec, BloscV3Codec, CRC32cV3Codec),
                            tag = R3)

zopen(sum, READER, "data/f64_2d_zstd.zarr")      # .zarray
zopen(sum, READER, "data/f64_2d_zstd_v3.zarr")   # zarr.json
```

The visitor looks for `.zarray` first and `zarr.json` second, in whichever of
the two the `zarr_format` pool asks for, and throws an `ArgumentError` naming
the formats it tried when it finds neither.

Two more names are emitted for a v3 reader. `BBPool_R3` is the bytes→bytes
codec pool — the v3 counterpart of `CodecPool_R3`, a
`V3Codec{:bytes,:bytes}` whose single `Union`-typed field is forwarded with
`isa` chains for `codec_encode`, `codec_decode`, `name`, `JSON.lower` and
`getCodec`. `P_R3` is the pipeline type its leaves ask for:

```julia
const P_R3 = ZarrCore.V3Pipeline{Tuple{}, ZarrCore.BytesCodec, Vector{BBPool_R3}}
```

Because the bytes→bytes stage is a `Vector` rather than a `Tuple`, **any
number of bytes→bytes codecs in any order** is accepted at no extra cost — a
`[bytes, zstd, crc32c]` chain works as well as a bare `[bytes]` one. A codec
type joins a `v3_codecs` pool once it defines
`V3Codecs.codec_name(::Type{C})` and
`V3Codecs.getCodec(::Type{C}, ::ZarrCore.CodecJSON, ctx)` alongside
`codec_encode`, `codec_decode` and `JSON.lower`.

## Admitting your own compressor

A compressor type `C <: ZarrCore.Compressor` can be a member of a codec pool
when it implements the typed compressor interface plus one identity method:

- `ZarrCore.codec_id(::Type{C})::String` — the numcodecs `"id"` it is stored
  under in `.zarray` (`""` for `NoCompressor`, which is stored as `null`).
- `ZarrCore.getCompressor(::Type{C}, ::ZarrCore.CompressorJSON)`
- `ZarrCore.zcompress!`, `ZarrCore.zuncompress!`, `JSON.lower`

`ZarrCore` defines `codec_id` for `NoCompressor`; `ZarrZstd`, `ZarrZlib`
and `ZarrBlosc` define it for their compressors.

## What a pool costs

Measured with Julia 1.12.6, `juliac --trim=safe`, on x86-64 Linux; the
baseline is a reader over `{Float64, Int32} × {2, 3} × 4 codecs × {DirectoryStore}`
(5.7 MB, 0 verifier errors).

| axis | cost per extra member |
|---|---|
| codec | ≈ 6 KB (plus a one-time ≈ 45 KB when a new codec package is linked) |
| `(dtype, ndims)` pair | ≈ 175 KB (one more `ZArray` specialisation) |
| store | ≈ flat; only the forwarding chain grows |
| Zarr v3 alongside v2 | ≈ 1.5 MB for a `{Float64, Int32} × {2, 3}` reader (a second `MetadataV3` open/read stack per pair), measured as 5,032,048 B → 6,520,120 B for `progs/trimmable` → `progs/v3_typed/trimmable_v3` |

Per array, a pooled open is ≈ 10 µs slower than a direct typed `zopen`
(`.zarray` parse plus the dtype/rank/id chains); the read and write paths
are statically resolved and cost nothing measurable.

## Limitations

- Reading is Zarr v2 (without `filters`) and/or v3; **creating through a
  reader is v2 only**, and a v3-only reader that leaves `codecs` empty gets no
  `zcreate` methods at all. (A bare `zcreate(T, store, dims...; zarr_format = 3)`
  is trim-clean on its own; it is only the pooled `zcreate(reader, ...)` form
  that is v2-only.)
- The v3 path takes **bytes→bytes codecs only**: the array→bytes codec is
  always the plain `bytes` codec and the array→array stage is empty, so a
  stored `transpose`, `sharding_indexed` or `vlen-utf8` codec fails the open
  with an `ArgumentError`, as does a bytes→bytes codec outside `v3_codecs`.
- Chunk key encodings `default` and `v2` work on the v3 path (with any
  separator); `suffix` does not. The rest of the typed v3 path's limits apply
  as well: regular chunk grid only, no `storage_transformers`, no `null` fill
  value, and no object-spelled data types.
- The pool is not `NoCompressor`, so the zero-copy uncompressed write fast
  path (specialised on `MetadataV2{T,N,NoCompressor,Nothing}`) is not taken;
  writes go through `zcompress!`.
- `store_read_strategy` of a store pool is pinned to `SequentialRead()` by
  default; `read_strategy = :forward` forwards the members' real strategy
  but pulls the concurrent `Channel` read path, which is not trim-clean,
  into the program.
- `ConsolidatedStore`, `CachingStore` and the remote stores (`HTTPStore`,
  `S3Store`, `GCStore`) must not be pool members: the wrappers would make
  the pool recursive and bypass their own metadata overrides, and the remote
  stores force the concurrent read path. `ZipStore` reads correctly through
  a pool but is not trim-clean because of its ZipArchives dependency.
- `storefromstring` dispatches on the store type and cannot be mirrored by
  a pool; open by path uses `path_store` (default: the first store in the
  pool).
- Scalar indexing `z[i, j]` is not trim-clean (DiskArrays builds the index
  vector dynamically); use range indexing such as `z[:, :]` or `z[2:3, 4:6]`.
- `z.attrs` is an abstract field; assert `z.attrs::Dict{String,Any}` before
  indexing it in a compiled program.

## `ArrayMeta`

`ZarrTrimmable` also carries a closed-set, `Any`-free mirror of the `.zarray`
document, useful for inspecting or writing metadata from a compiled program
without touching `Dict{String,Any}`:

```julia
m = ZarrTrimmable.read_meta_path("data/f64_2d_zstd.zarr")
println(ZarrTrimmable.describe(m))
ZarrTrimmable.write_meta(stdout, m)   # byte-identical to what zcreate writes
```

`ArrayMeta` knows the element types `Float64`, `Float32`, `Int32`, `Int64`,
`UInt8`, the compressor specs `NoComp`, `Zstd`, `Zlib`, `Blosc` (by id only;
the codec packages are not required) and the fill values `null`, number,
`NaN` and integer.
