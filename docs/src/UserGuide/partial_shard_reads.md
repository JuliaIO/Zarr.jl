# Partial reads of sharded arrays

Zarr v3's `sharding_indexed` codec packs many small inner chunks into one
larger outer chunk file, with an index that records each inner chunk's
byte offset and length inside the file. The index lets a reader fetch
just the inner chunks intersecting a user's slice instead of decoding
the whole outer chunk.

`Zarr.jl` exploits this in two layers:

1. **In-memory partial decode** — when an outer chunk has been read into
   memory (the existing path), only decode the inner chunks the request
   actually touches. Skips decompression for unrelated inner chunks.
2. **Storage-aware partial read** — when the storage backend supports
   byte-range reads, fetch only the shard index plus the bytes of the
   intersecting inner chunks. Skips both the I/O and the decode for
   everything else.

Both paths are transparent to user code — `arr[a:b, c]` is the same call
whether the array is sharded or not. The fast paths kick in
automatically when the codec pipeline is "pure" sharding (no
array→array codecs before the sharding codec, no bytes→bytes codecs
wrapping it).

## When the fast path applies

The fast path is taken when:

- The codec pipeline is exactly one `ShardingCodec` (no surrounding
  codecs). This is the common shape that `zarr-python` produces and
  that practical sharded archives use.
- The requested slice is *partial* — i.e. it covers fewer elements than
  the outer chunk shape. For full-chunk reads the existing path is
  already optimal (the whole shard's bytes need to come off disk and
  be decoded anyway).

When the pipeline has additional codecs (e.g. transpose, blosc-around-
sharding) or when the request happens to align exactly with an outer
chunk, the existing decode path runs unchanged.

## Storage backends

Stores opt into the storage-aware path by implementing three optional
methods. The defaults provided by `AbstractStore` keep correctness for
backends that don't implement them — any such store falls back to the
in-memory partial-decode path automatically.

```@docs
Zarr.supports_partial_reads
Zarr.read_range
Zarr.getsize
```

`DirectoryStore` opts in (using `seek` + `readbytes!` for `read_range`
and `filesize` for `getsize`). Other built-in backends inherit the
defaults; adding native byte-range support to e.g. `S3Store` or
`HTTPStore` is a one-method change for each, since both wire protocols
support byte ranges natively.

## Toggle

```@docs
Zarr.enable_partial_shard_storage_reads
```

Set the flag to `false` to fall back to the in-memory partial-decode
path even on stores that support byte-range reads. Useful for A/B
performance comparisons or to debug a suspected partial-read bug.

## Threading

When Julia is started with more than one thread (`julia -t N`), the
sharded partial-read path internally dispatches inner-chunk decodes to
`Threads.@spawn` so a single `arr[a:b, c]` call scales with available
cores — the same way `zarr-python` parallelizes inner-chunk decodes
inside one `__getitem__`. User code is unchanged; the threading is
transparent.

Two layers of parallelism kick in automatically:

1. Within one outer chunk, the inner chunks intersecting the request
   decode in parallel (bounded by `max_concurrent_inner_decodes[]`).
2. Across outer chunks (when the request spans more than one), the
   per-chunk reads dispatch in parallel.

Both fall back to a sequential loop on single-threaded runs or when
the work list has fewer than two entries.

```@docs
Zarr.enable_threaded_shard_decode
Zarr.max_concurrent_inner_decodes
```

## In-place codec API

The decode pipeline avoids per-inner-chunk transient allocations by
threading the caller's output buffer through each codec via
`Zarr.Codecs.V3Codecs.codec_decode!`. For the dominant pipeline shape
`[BytesCodec, ZstdV3Codec]` (matching system endian), the inner chunk
decompresses straight into `reinterpret(UInt8, vec(output))` — no
intermediate `Vector{UInt8}` and no second copy.

`codec_decode!` is dispatched on the codec's `In/Out` tag pair so any
new codec written for `Zarr.jl` can opt in by adding a specialized
method. A generic fallback (allocate + `copyto!`) is provided for
`V3Codec{:bytes,:bytes}`, `V3Codec{:array,:bytes}`, and
`V3Codec{:array,:array}`, so codecs without a specialization remain
correct, just less alloc-friendly.

## Reference

The storage-aware path lives in `Zarr._readblock_sharded_partial!`
(in `src/ZArray.jl`). The shared decode loop used by both paths is
`Zarr.Codecs.V3Codecs.read_shard_partial_with_source!` (in
`src/Codecs/V3/V3.jl`); the in-memory wrapper is
`Zarr.Codecs.V3Codecs.read_shard_partial!`. The pipeline detection
helper that distinguishes "pure" sharding from compound pipelines is
`Zarr.Codecs.V3Codecs.sharding_codec`.
