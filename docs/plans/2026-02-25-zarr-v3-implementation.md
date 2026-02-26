# Zarr V3 Codec Pipeline Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Implement a proper v3 codec pipeline so Zarr.jl can read (and write non-sharded) zarr-v3 data with full Python interoperability.

**Architecture:** Introduce `AbstractCodecPipeline` with `V2Pipeline` (wrapping existing compressor+filters) and `V3Pipeline` (three-phase typed codec chain). `ZArray`'s chunk I/O calls through the pipeline abstraction. `MetadataV3` stores the pipeline directly instead of extracting a single compressor.

**Tech Stack:** Julia 1.10+, existing Zarr.jl dependencies (Blosc, ChunkCodecLibZlib, ChunkCodecLibZstd, CRC32c, JSON, DiskArrays)

**Design doc:** `docs/plans/2026-02-25-zarr-v3-codec-pipeline-design.md`

---

### Task 1: Simplify AbstractMetadata Type Parameters

Remove `C,F` from `AbstractMetadata` supertype since `ZArray` only uses `{T,N}`. `MetadataV2` keeps its own `{T,N,C,F}` parameters.

**Files:**
- Modify: `src/metadata.jl:106-107` (AbstractMetadata definition)
- Modify: `src/metadata.jl:111` (MetadataV2 supertype)
- Modify: `src/metadata.jl:134` (MetadataV3 supertype)
- Test: `test/runtests.jl` (existing tests must still pass)

**Step 1: Change AbstractMetadata definition**

In `src/metadata.jl`, change:
```julia
abstract type AbstractMetadata{T,N,C,F} end
```
to:
```julia
abstract type AbstractMetadata{T,N} end
```

**Step 2: Update MetadataV2 supertype**

Change:
```julia
struct MetadataV2{T,N,C,F} <: AbstractMetadata{T,N,C,F}
```
to:
```julia
struct MetadataV2{T,N,C,F} <: AbstractMetadata{T,N}
```

**Step 3: Update MetadataV3 supertype**

Change:
```julia
struct MetadataV3{T,N,C,F} <: AbstractMetadata{T,N,C,F}
```
to:
```julia
struct MetadataV3{T,N,C,F} <: AbstractMetadata{T,N}
```

(We'll change MetadataV3's own type params in a later task.)

**Step 4: Update the Metadata type alias**

The line `const Metadata = AbstractMetadata` at `src/metadata.jl:157` stays — it's just an alias.

**Step 5: Run tests to verify no breakage**

Run: `julia --project -e 'using Pkg; Pkg.test()'`
Expected: All existing tests pass. The `ZArray{T,N,S,M}` constraint is `M<:AbstractMetadata{T,N}` which still works since `MetadataV2{T,N,C,F} <: AbstractMetadata{T,N}`.

**Step 6: Commit**

```bash
git add src/metadata.jl
git commit -m "refactor: simplify AbstractMetadata to {T,N} type parameters"
```

---

### Task 2: Implement V3 Codec Types with encode/decode

The existing `V3Codecs` module in `Codecs/V3/V3.jl` has struct definitions but lacks proper encode/decode for most codecs. Add `codec_encode`/`codec_decode` methods that the pipeline will call.

**Files:**
- Modify: `src/Codecs/V3/V3.jl` (add encode/decode methods, fix naming conflicts)
- Create: `test/v3_codecs.jl` (unit tests for each codec)
- Modify: `test/runtests.jl` (include new test file)

**Step 1: Write failing tests for BytesCodec**

Create `test/v3_codecs.jl`:

```julia
using Test
using Zarr
using Zarr.Codecs.V3Codecs

@testset "V3 Codecs" begin

@testset "BytesCodec" begin
    codec = Zarr.Codecs.V3Codecs.BytesCodec()
    data = Int32[1, 2, 3, 4]
    encoded = Zarr.Codecs.V3Codecs.codec_encode(codec, data)
    @test encoded isa Vector{UInt8}
    @test length(encoded) == 16  # 4 * sizeof(Int32)
    decoded = Zarr.Codecs.V3Codecs.codec_decode(codec, encoded, Int32, (4,))
    @test decoded == data
end

end # V3 Codecs
```

Add to `test/runtests.jl` before the final `end`:
```julia
include("v3_codecs.jl")
```

**Step 2: Run test to verify it fails**

Run: `julia --project -e 'using Test, Zarr; include("test/v3_codecs.jl")'`
Expected: FAIL — `codec_encode` not defined.

**Step 3: Implement BytesCodec encode/decode**

In `src/Codecs/V3/V3.jl`, add after the `BytesCodec` struct definition (line 36):

```julia
function codec_encode(::BytesCodec, data::AbstractArray)
    return reinterpret(UInt8, vec(data)) |> collect
end

function codec_decode(::BytesCodec, encoded::Vector{UInt8}, ::Type{T}, shape::NTuple{N,Int}) where {T, N}
    arr = reinterpret(T, encoded)
    return reshape(collect(arr), shape)
end
```

**Step 4: Run test to verify it passes**

Run: `julia --project -e 'using Test, Zarr; include("test/v3_codecs.jl")'`
Expected: PASS

**Step 5: Add TransposeCodec tests and implementation**

Add to `test/v3_codecs.jl`:

```julia
@testset "TransposeCodec" begin
    # C-order (identity permutation for Julia column-major)
    codec_c = Zarr.Codecs.V3Codecs.TransposeCodecImpl((1, 2, 3))
    data = reshape(collect(1:24), 2, 3, 4)
    encoded = Zarr.Codecs.V3Codecs.codec_encode(codec_c, data)
    @test encoded == data  # identity perm = no change

    # Reverse permutation (F-order)
    codec_f = Zarr.Codecs.V3Codecs.TransposeCodecImpl((3, 2, 1))
    encoded_f = Zarr.Codecs.V3Codecs.codec_encode(codec_f, data)
    decoded_f = Zarr.Codecs.V3Codecs.codec_decode(codec_f, encoded_f)
    @test decoded_f == data
end
```

In `src/Codecs/V3/V3.jl`, replace the existing `TransposeCodec` struct with:

```julia
struct TransposeCodecImpl{N} <: V3Codec{:array, :array}
    order::NTuple{N, Int}  # permutation (1-based)
end
name(::TransposeCodecImpl) = "transpose"

function codec_encode(c::TransposeCodecImpl, data::AbstractArray)
    return permutedims(data, c.order)
end

function codec_decode(c::TransposeCodecImpl, encoded::AbstractArray)
    # Inverse permutation
    inv_order = invperm(collect(c.order)) |> Tuple
    return permutedims(encoded, inv_order)
end
```

**Step 6: Add bytes->bytes codec tests and implementations**

Add to `test/v3_codecs.jl`:

```julia
@testset "GzipV3Codec" begin
    codec = Zarr.Codecs.V3Codecs.GzipV3Codec(6)
    data = reinterpret(UInt8, Int32[1, 2, 3, 4]) |> collect
    encoded = Zarr.Codecs.V3Codecs.codec_encode(codec, data)
    @test encoded isa Vector{UInt8}
    @test encoded != data  # should be compressed
    decoded = Zarr.Codecs.V3Codecs.codec_decode(codec, encoded)
    @test decoded == data
end

@testset "BloscV3Codec" begin
    codec = Zarr.Codecs.V3Codecs.BloscV3Codec("lz4", 5, 0, 0, 4)
    data = reinterpret(UInt8, Int32[1, 2, 3, 4]) |> collect
    encoded = Zarr.Codecs.V3Codecs.codec_encode(codec, data)
    @test encoded isa Vector{UInt8}
    decoded = Zarr.Codecs.V3Codecs.codec_decode(codec, encoded)
    @test decoded == data
end

@testset "ZstdV3Codec" begin
    codec = Zarr.Codecs.V3Codecs.ZstdV3Codec(3)
    data = reinterpret(UInt8, Float64[1.5, 2.5, 3.5, 4.5]) |> collect
    encoded = Zarr.Codecs.V3Codecs.codec_encode(codec, data)
    @test encoded isa Vector{UInt8}
    decoded = Zarr.Codecs.V3Codecs.codec_decode(codec, encoded)
    @test decoded == data
end

@testset "CRC32cV3Codec" begin
    codec = Zarr.Codecs.V3Codecs.CRC32cV3Codec()
    data = UInt8[1, 2, 3, 4, 5, 6, 7, 8]
    encoded = Zarr.Codecs.V3Codecs.codec_encode(codec, data)
    @test length(encoded) == length(data) + 4  # 4 bytes for checksum
    decoded = Zarr.Codecs.V3Codecs.codec_decode(codec, encoded)
    @test decoded == data
end
```

In `src/Codecs/V3/V3.jl`, add new wrapper codec structs that delegate to existing compressors and add `codec_encode`/`codec_decode` interface. Note: we create new structs (with `V3` prefix) to avoid name conflicts with the existing `BloscCompressor` enum and `Compressor` types:

```julia
# Wrapper codecs that delegate to existing Compressor implementations
struct GzipV3Codec <: V3Codec{:bytes, :bytes}
    level::Int
end
GzipV3Codec() = GzipV3Codec(6)
name(::GzipV3Codec) = "gzip"

function codec_encode(c::GzipV3Codec, data::Vector{UInt8})
    comp = Zarr.ZlibCompressor(clevel=c.level)
    return Zarr.zcompress(data, comp)
end
function codec_decode(c::GzipV3Codec, encoded::Vector{UInt8})
    comp = Zarr.ZlibCompressor(clevel=c.level)
    return Zarr.zuncompress(encoded, comp, UInt8) |> collect
end

struct BloscV3Codec <: V3Codec{:bytes, :bytes}
    cname::String
    clevel::Int
    shuffle::Int
    blocksize::Int
    typesize::Int
end
BloscV3Codec() = BloscV3Codec("lz4", 5, 1, 0, 4)
name(::BloscV3Codec) = "blosc"

function codec_encode(c::BloscV3Codec, data::Vector{UInt8})
    comp = Zarr.BloscCompressor(blocksize=c.blocksize, clevel=c.clevel, cname=c.cname, shuffle=c.shuffle > 0)
    return Zarr.zcompress(data, comp)
end
function codec_decode(c::BloscV3Codec, encoded::Vector{UInt8})
    comp = Zarr.BloscCompressor()
    return Zarr.zuncompress(encoded, comp, UInt8) |> collect
end

struct ZstdV3Codec <: V3Codec{:bytes, :bytes}
    level::Int
end
ZstdV3Codec() = ZstdV3Codec(3)
name(::ZstdV3Codec) = "zstd"

function codec_encode(c::ZstdV3Codec, data::Vector{UInt8})
    comp = Zarr.ZstdCompressor(clevel=c.level)
    return Zarr.zcompress(data, comp)
end
function codec_decode(c::ZstdV3Codec, encoded::Vector{UInt8})
    comp = Zarr.ZstdCompressor(clevel=c.level)
    return Zarr.zuncompress(encoded, comp, UInt8) |> collect
end

struct CRC32cV3Codec <: V3Codec{:bytes, :bytes}
end
name(::CRC32cV3Codec) = "crc32c"

function codec_encode(c::CRC32cV3Codec, data::Vector{UInt8})
    out = UInt8[]
    zencode!(out, data, CRC32cCodec())
    return out
end
function codec_decode(c::CRC32cV3Codec, encoded::Vector{UInt8})
    out = UInt8[]
    zdecode!(out, encoded, CRC32cCodec())
    return out
end
```

Remove or rename the old `GzipCodec` and `CRC32cCodec` struct definitions that conflict, keeping the CRC32cCodec only for its `zencode!/zdecode!` implementations.

**Step 7: Run all codec tests**

Run: `julia --project -e 'using Test, Zarr; include("test/v3_codecs.jl")'`
Expected: All PASS

**Step 8: Run full test suite**

Run: `julia --project -e 'using Pkg; Pkg.test()'`
Expected: All existing tests still pass

**Step 9: Commit**

```bash
git add src/Codecs/V3/V3.jl test/v3_codecs.jl test/runtests.jl
git commit -m "feat: add codec_encode/codec_decode for all v3 codecs"
```

---

### Task 3: Create CodecPipeline Abstraction

**Files:**
- Create: `src/pipeline.jl`
- Modify: `src/Zarr.jl` (include new file)
- Create: `test/pipeline.jl` (pipeline unit tests)
- Modify: `test/runtests.jl` (include new test file)

**Step 1: Write failing tests for V2Pipeline**

Create `test/pipeline.jl`:

```julia
using Test
using Zarr

@testset "CodecPipeline" begin

@testset "V2Pipeline encode/decode round-trip" begin
    comp = Zarr.BloscCompressor()
    pipeline = Zarr.V2Pipeline(comp, nothing)
    data = zeros(Int64, 4, 4)
    data[1, 1] = 42

    encoded = Zarr.pipeline_encode(pipeline, data, nothing)
    @test encoded isa Vector{UInt8}
    @test !isempty(encoded)

    output = zeros(Int64, 4, 4)
    Zarr.pipeline_decode!(pipeline, output, encoded)
    @test output == data
end

@testset "V2Pipeline with fill_value returns nothing" begin
    comp = Zarr.BloscCompressor()
    pipeline = Zarr.V2Pipeline(comp, nothing)
    data = fill(Int64(-1), 4, 4)
    encoded = Zarr.pipeline_encode(pipeline, data, Int64(-1))
    @test encoded === nothing
end

@testset "V3Pipeline encode/decode round-trip" begin
    bytes_codec = Zarr.Codecs.V3Codecs.BytesCodec()
    gzip_codec = Zarr.Codecs.V3Codecs.GzipV3Codec(6)
    pipeline = Zarr.V3Pipeline((), bytes_codec, (gzip_codec,))

    data = Int32[1, 2, 3, 4]
    encoded = Zarr.pipeline_encode(pipeline, data, nothing)
    @test encoded isa Vector{UInt8}

    output = zeros(Int32, 4)
    Zarr.pipeline_decode!(pipeline, output, encoded)
    @test output == data
end

end # CodecPipeline
```

**Step 2: Run test to verify it fails**

Run: `julia --project -e 'using Test, Zarr; include("test/pipeline.jl")'`
Expected: FAIL — `V2Pipeline` not defined.

**Step 3: Implement pipeline.jl**

Create `src/pipeline.jl`:

```julia
abstract type AbstractCodecPipeline end

"""
V2Pipeline wraps the existing v2 compressor + filter pair.
Delegates to zcompress!/zuncompress! with zero behavior change.
"""
struct V2Pipeline{C<:Compressor, F} <: AbstractCodecPipeline
    compressor::C
    filters::F
end

function pipeline_encode(p::V2Pipeline, data::AbstractArray, fill_value)
    if fill_value !== nothing && all(isequal(fill_value), data)
        return nothing
    end
    dtemp = UInt8[]
    zcompress!(dtemp, data, p.compressor, p.filters)
    return dtemp
end

function pipeline_decode!(p::V2Pipeline, output::AbstractArray, compressed::Vector{UInt8})
    zuncompress!(output, compressed, p.compressor, p.filters)
    return output
end

"""
V3Pipeline holds a three-phase v3 codec chain:
- array_array: tuple of array→array codecs (e.g. transpose)
- array_bytes: single array→bytes codec (e.g. bytes, sharding)
- bytes_bytes: tuple of bytes→bytes codecs (e.g. gzip, blosc, crc32c)
"""
struct V3Pipeline{AA, AB, BB} <: AbstractCodecPipeline
    array_array::AA
    array_bytes::AB
    bytes_bytes::BB
end

function pipeline_encode(p::V3Pipeline, data::AbstractArray, fill_value)
    if fill_value !== nothing && all(isequal(fill_value), data)
        return nothing
    end
    # Phase 1: array→array codecs (forward order)
    result = data
    for codec in p.array_array
        result = Codecs.V3Codecs.codec_encode(codec, result)
    end
    # Phase 2: array→bytes codec
    bytes = Codecs.V3Codecs.codec_encode(p.array_bytes, result)
    # Phase 3: bytes→bytes codecs (forward order)
    for codec in p.bytes_bytes
        bytes = Codecs.V3Codecs.codec_encode(codec, bytes)
    end
    return bytes
end

function pipeline_decode!(p::V3Pipeline, output::AbstractArray, compressed::Vector{UInt8})
    # Phase 3 reverse: bytes→bytes codecs (reverse order)
    bytes = compressed
    for codec in reverse(collect(p.bytes_bytes))
        bytes = Codecs.V3Codecs.codec_decode(codec, bytes)
    end
    # Phase 2 reverse: bytes→array codec
    arr = Codecs.V3Codecs.codec_decode(p.array_bytes, bytes, eltype(output), size(output))
    # Phase 1 reverse: array→array codecs (reverse order)
    for codec in reverse(collect(p.array_array))
        arr = Codecs.V3Codecs.codec_decode(codec, arr)
    end
    copyto!(output, arr)
    return output
end

# Convenience: extract pipeline from metadata
get_pipeline(m::MetadataV2) = V2Pipeline(m.compressor, m.filters)
```

**Step 4: Include pipeline.jl in Zarr.jl**

In `src/Zarr.jl`, add `include("pipeline.jl")` after the `include("ZArray.jl")` line (line 22). The pipeline needs access to `Compressor` types (from Compressors), `Codecs` module, and `MetadataV2`:

```julia
include("ZArray.jl")
include("pipeline.jl")
include("ZGroup.jl")
```

**Step 5: Run tests**

Run: `julia --project -e 'using Test, Zarr; include("test/pipeline.jl")'`
Expected: All PASS

**Step 6: Run full test suite**

Run: `julia --project -e 'using Pkg; Pkg.test()'`
Expected: All existing tests still pass

**Step 7: Commit**

```bash
git add src/pipeline.jl src/Zarr.jl test/pipeline.jl test/runtests.jl
git commit -m "feat: add CodecPipeline abstraction (V2Pipeline, V3Pipeline)"
```

---

### Task 4: Redesign MetadataV3 to Store V3Pipeline

**Files:**
- Modify: `src/metadata.jl:134-153` (MetadataV3 struct)
- Modify: `src/metadata3.jl` (Metadata3 parsing builds V3Pipeline)
- Modify: `src/pipeline.jl` (add `get_pipeline` for MetadataV3)
- Test: `test/v3_codecs.jl` (add metadata parsing tests)

**Step 1: Write failing test for v3 metadata parsing**

Add to `test/v3_codecs.jl`:

```julia
@testset "V3 Metadata Parsing" begin
    json_str = """{
        "zarr_format": 3,
        "node_type": "array",
        "shape": [4],
        "data_type": "int32",
        "chunk_grid": {"name": "regular", "configuration": {"chunk_shape": [4]}},
        "chunk_key_encoding": {"name": "default", "configuration": {"separator": "/"}},
        "fill_value": 0,
        "codecs": [
            {"name": "transpose", "configuration": {"order": [0]}},
            {"name": "bytes", "configuration": {"endian": "little"}},
            {"name": "gzip", "configuration": {"level": 6}}
        ]
    }"""
    md = Zarr.Metadata(json_str, false)
    @test md isa Zarr.MetadataV3
    @test md.shape[] == (4,)
    @test md.chunks == (4,)
    @test md.fill_value == Int32(0)

    pipeline = Zarr.get_pipeline(md)
    @test pipeline isa Zarr.V3Pipeline
    @test length(pipeline.array_array) == 1  # transpose
    @test pipeline.array_bytes isa Zarr.Codecs.V3Codecs.BytesCodec
    @test length(pipeline.bytes_bytes) == 1  # gzip
end
```

**Step 2: Run test to verify it fails**

Run: `julia --project -e 'using Test, Zarr; include("test/v3_codecs.jl")'`
Expected: FAIL — MetadataV3 doesn't have a pipeline field.

**Step 3: Modify MetadataV3 struct**

In `src/metadata.jl`, change the MetadataV3 struct (lines 134-153):

```julia
"""Metadata for Zarr version 3 arrays"""
struct MetadataV3{T,N,P<:AbstractCodecPipeline} <: AbstractMetadata{T,N}
    zarr_format::Int
    node_type::String
    shape::Base.RefValue{NTuple{N, Int}}
    chunks::NTuple{N, Int}
    dtype::String
    pipeline::P
    fill_value::Union{T, Nothing}
    order::Char
    chunk_encoding::ChunkEncoding
    function MetadataV3{T2,N,P}(zarr_format, node_type, shape, chunks, dtype, pipeline, fill_value, order, chunk_encoding) where {T2,N,P}
        zarr_format == 3 || throw(ArgumentError("MetadataV3 only functions if zarr_format == 3"))
        any(<(0), shape) && throw(ArgumentError("Size must be positive"))
        any(<(1), chunks) && throw(ArgumentError("Chunk size must be >= 1 along each dimension"))
        new{T2,N,P}(zarr_format, node_type, Base.RefValue{NTuple{N,Int}}(shape), chunks, dtype, pipeline, fill_value, order, chunk_encoding)
    end
end
```

**Step 4: Update Metadata3(dict) in metadata3.jl to build V3Pipeline**

Rewrite the codec parsing section of `Metadata3(d::AbstractDict, fill_as_missing)` to build a `V3Pipeline` instead of extracting a single compressor. The function should:

1. Iterate through `d["codecs"]`
2. Categorize each into `array_array`, `array_bytes`, or `bytes_bytes`
3. Construct codec objects (`BytesCodec`, `GzipV3Codec`, `BloscV3Codec`, etc.)
4. Build `V3Pipeline(tuple(array_array...), array_bytes, tuple(bytes_bytes...))`
5. Construct `MetadataV3{TU, N, typeof(pipeline)}(..., pipeline, ...)`

Key parsing logic:

```julia
array_array_codecs = []
array_bytes_codec = nothing
bytes_bytes_codecs = []

for codec in d["codecs"]
    codec_name = codec["name"]
    config = get(codec, "configuration", Dict{String,Any}())
    if codec_name == "transpose"
        order = config["order"]
        if order isa AbstractString
            # Handle deprecated "C"/"F" strings
            n = length(shape)
            perm = order == "C" ? ntuple(identity, n) : ntuple(i -> n - i + 1, n)
        else
            perm = Tuple(Int.(order) .+ 1)
        end
        push!(array_array_codecs, Codecs.V3Codecs.TransposeCodecImpl(perm))
    elseif codec_name == "bytes"
        if haskey(config, "endian")
            config["endian"] == "little" || throw(ArgumentError("Only little endian supported"))
        end
        array_bytes_codec = Codecs.V3Codecs.BytesCodec()
    elseif codec_name == "sharding_indexed"
        throw(ArgumentError("Sharding read support not yet wired in")) # Task 5 will fix
    elseif codec_name == "gzip"
        level = get(config, "level", 6)
        push!(bytes_bytes_codecs, Codecs.V3Codecs.GzipV3Codec(level))
    elseif codec_name == "blosc"
        cname = get(config, "cname", "lz4")
        clevel = get(config, "clevel", 5)
        shuffle_val = get(config, "shuffle", "noshuffle")
        shuffle_int = shuffle_val isa Integer ? shuffle_val :
                      shuffle_val == "noshuffle" ? 0 :
                      shuffle_val == "shuffle" ? 1 :
                      shuffle_val == "bitshuffle" ? 2 : 0
        blocksize = get(config, "blocksize", 0)
        typesize = get(config, "typesize", 4)
        push!(bytes_bytes_codecs, Codecs.V3Codecs.BloscV3Codec(cname, clevel, shuffle_int, blocksize, typesize))
    elseif codec_name == "zstd"
        level = get(config, "level", 3)
        push!(bytes_bytes_codecs, Codecs.V3Codecs.ZstdV3Codec(level))
    elseif codec_name == "crc32c"
        push!(bytes_bytes_codecs, Codecs.V3Codecs.CRC32cV3Codec())
    else
        throw(ArgumentError("Unsupported codec: $codec_name"))
    end
end

isnothing(array_bytes_codec) && throw(ArgumentError("V3 codec chain must have an array->bytes codec (e.g. 'bytes')"))
pipeline = V3Pipeline(Tuple(array_array_codecs), array_bytes_codec, Tuple(bytes_bytes_codecs))
```

Also update the `Metadata3(A::AbstractArray, ...)` constructor to build a pipeline from the given compressor.

**Step 5: Update lower3 serialization**

Update `lower3` in `metadata3.jl` to reconstruct the codec chain from the pipeline:

```julia
function lower3(md::MetadataV3{T}) where T
    codecs = Dict{String,Any}[]
    p = md.pipeline

    # array→array codecs
    for codec in p.array_array
        if codec isa Codecs.V3Codecs.TransposeCodecImpl
            push!(codecs, Dict{String,Any}(
                "name" => "transpose",
                "configuration" => Dict("order" => collect(codec.order .- 1))
            ))
        end
    end

    # array→bytes codec
    if p.array_bytes isa Codecs.V3Codecs.BytesCodec
        push!(codecs, Dict{String,Any}(
            "name" => "bytes",
            "configuration" => Dict("endian" => "little")
        ))
    end

    # bytes→bytes codecs
    for codec in p.bytes_bytes
        push!(codecs, JSON.lower(codec))
    end

    # ... rest of metadata dict construction
end
```

Add `JSON.lower` methods for each v3 codec in `V3/V3.jl`.

**Step 6: Update get_pipeline for MetadataV3**

In `src/pipeline.jl`, add:

```julia
get_pipeline(m::MetadataV3) = m.pipeline
```

**Step 7: Update the `==` method for AbstractMetadata**

The `==` method in `metadata.jl` references `m.compressor` and `m.filters` which no longer exist on MetadataV3. Split into separate methods or update to compare pipeline for v3.

**Step 8: Run tests**

Run: `julia --project -e 'using Test, Zarr; include("test/v3_codecs.jl")'`
Expected: V3 Metadata Parsing test passes.

**Step 9: Run full test suite**

Run: `julia --project -e 'using Pkg; Pkg.test()'`
Expected: All tests pass.

**Step 10: Commit**

```bash
git add src/metadata.jl src/metadata3.jl src/pipeline.jl test/v3_codecs.jl
git commit -m "feat: MetadataV3 stores V3Pipeline instead of compressor+filters"
```

---

### Task 5: Integrate Pipeline into ZArray Chunk I/O

**Files:**
- Modify: `src/ZArray.jl:274-308` (`uncompress_raw!` and `compress_raw`)
- Test: `test/v3_codecs.jl` (add round-trip ZArray v3 test)

**Step 1: Write failing test for v3 ZArray round-trip**

Add to `test/v3_codecs.jl`:

```julia
@testset "V3 ZArray round-trip" begin
    z = zcreate(Int32, 8; zarr_format=3, chunks=(4,), fill_value=Int32(0))
    z[:] = Int32.(1:8)
    @test z[:] == Int32.(1:8)
    @test z[3:6] == Int32[3, 4, 5, 6]
end

@testset "V3 ZArray with gzip" begin
    z = zcreate(Float64, 4, 4; zarr_format=3, chunks=(2, 2),
        compressor=Zarr.ZlibCompressor(), fill_value=0.0)
    z[:, :] = reshape(Float64.(1:16), 4, 4)
    @test z[:, :] == reshape(Float64.(1:16), 4, 4)
end
```

**Step 2: Run test to verify it fails**

Run: `julia --project -e 'using Test, Zarr; include("test/v3_codecs.jl")'`
Expected: FAIL — `uncompress_raw!` tries to access `z.metadata.compressor` which doesn't exist on MetadataV3.

**Step 3: Modify uncompress_raw! and compress_raw**

In `src/ZArray.jl`, change `uncompress_raw!` (line 274) to use the pipeline:

```julia
function uncompress_raw!(a, z::ZArray{<:Any,N}, curchunk) where N
    if curchunk === nothing
        fv = z.metadata.fill_value
        if isnothing(fv)
            throw(ArgumentError("The array $z got missing chunks and no fill_value"))
        end
        fill!(a, fv)
    else
        pipeline_decode!(get_pipeline(z.metadata), a, curchunk)
    end
    a
end
```

Change `compress_raw` (line 300):

```julia
function compress_raw(a, z)
    length(a) == prod(z.metadata.chunks) || throw(DimensionMismatch("Array size does not equal chunk size"))
    pipeline_encode(get_pipeline(z.metadata), a, z.metadata.fill_value)
end
```

**Step 4: Run tests**

Run: `julia --project -e 'using Test, Zarr; include("test/v3_codecs.jl")'`
Expected: V3 ZArray tests pass.

**Step 5: Run full test suite to verify v2 still works**

Run: `julia --project -e 'using Pkg; Pkg.test()'`
Expected: All tests pass — v2 goes through `V2Pipeline` which delegates to existing `zcompress!/zuncompress!`.

**Step 6: Commit**

```bash
git add src/ZArray.jl test/v3_codecs.jl
git commit -m "feat: ZArray uses CodecPipeline for chunk I/O"
```

---

### Task 6: V3 Group Creation

**Files:**
- Modify: `src/ZGroup.jl:156-170` (`zgroup` function)
- Modify: `src/Storage/Storage.jl` (add `write_group_metadata` dispatch)
- Test: `test/v3_codecs.jl` (add v3 group test)

**Step 1: Write failing test**

Add to `test/v3_codecs.jl`:

```julia
@testset "V3 Group Creation" begin
    store = Zarr.DictStore()
    g = zgroup(store, "", Zarr.ZarrFormat(3))
    @test haskey(store, "zarr.json")
    md = JSON.parse(String(copy(store["zarr.json"])))
    @test md["zarr_format"] == 3
    @test md["node_type"] == "group"

    # Create subgroup
    g2 = zgroup(g, "sub", attrs=Dict("key" => "val"))
    md2 = JSON.parse(String(copy(store["sub/zarr.json"])))
    @test md2["zarr_format"] == 3
    @test md2["node_type"] == "group"
    @test md2["attributes"]["key"] == "val"
end
```

**Step 2: Run test to verify it fails**

Run: `julia --project -e 'using Test, Zarr, JSON; include("test/v3_codecs.jl")'`
Expected: FAIL — `zgroup` always writes `.zgroup`.

**Step 3: Implement v3 group creation**

In `src/ZGroup.jl`, replace the `zgroup(s::AbstractStore, ...)` function (line 156-170):

```julia
function zgroup(s::AbstractStore, path::String="", zarr_format=ZarrFormat(2); attrs=Dict(), indent_json::Bool=false)
    zv = ZarrFormat(zarr_format)
    isemptysub(s, path) || error("Store is not empty")
    write_group_metadata(zv, s, path, attrs; indent_json=indent_json)
    ZGroup(s, path, Dict{String,ZArray}(), Dict{String,ZGroup}(), attrs, true)
end

function write_group_metadata(::ZarrFormat{2}, s::AbstractStore, path, attrs; indent_json::Bool=false)
    d = Dict("zarr_format" => 2)
    b = IOBuffer()
    indent_json ? JSON.print(b, d, 4) : JSON.print(b, d)
    s[path, ".zgroup"] = take!(b)
    writeattrs(ZarrFormat(Val(2)), s, path, attrs, indent_json=indent_json)
end

function write_group_metadata(::ZarrFormat{3}, s::AbstractStore, path, attrs; indent_json::Bool=false)
    d = Dict{String,Any}("zarr_format" => 3, "node_type" => "group")
    if !isempty(attrs)
        d["attributes"] = attrs
    end
    b = IOBuffer()
    indent_json ? JSON.print(b, d, 4) : JSON.print(b, d)
    s[path, "zarr.json"] = take!(b)
end
```

Also update the subgroup creation `zgroup(g::ZGroup, name; ...)` (line 175-178) to pass through the zarr format. Detect format from parent group's storage:

```julia
function zgroup(g::ZGroup, name; attrs=Dict())
    g.writeable || throw(ArgumentError("Not writeable"))
    subpath = _concatpath(g.path, name)
    # Detect format from parent
    zv = is_zarr3(g.storage, g.path) ? ZarrFormat(3) : ZarrFormat(2)
    g.groups[name] = zgroup(g.storage, subpath, zv, attrs=attrs)
end
```

**Step 4: Run tests**

Run: `julia --project -e 'using Test, Zarr, JSON; include("test/v3_codecs.jl")'`
Expected: V3 Group Creation test passes.

**Step 5: Run full test suite**

Run: `julia --project -e 'using Pkg; Pkg.test()'`
Expected: All tests pass.

**Step 6: Commit**

```bash
git add src/ZGroup.jl test/v3_codecs.jl
git commit -m "feat: zgroup() supports creating v3 groups via zarr.json"
```

---

### Task 7: Fix Test Fixtures

**Files:**
- Modify: `test/v3_julia.jl` (remove FormattedStore, use DirectoryStore + zarr_format=3)
- Test: Run the fixture generator

**Step 1: Update v3_julia.jl to remove FormattedStore**

Replace the store creation (line 16-21):

```julia
# Old:
# store = Zarr.FormattedStore{3, '/'}(Zarr.DirectoryStore(path_v3))

# New:
store = Zarr.DirectoryStore(path_v3)
```

Replace the root group creation (lines 17-21):

```julia
# Old: manual zarr.json creation
# New: use the updated zgroup
g = zgroup(store, "", Zarr.ZarrFormat(3))
```

In the `create_and_fill` helper, ensure `zarr_format=3` is passed through (it already is in the kwargs).

**Step 2: Run the fixture generator**

Run: `julia --project test/v3_julia.jl`
Expected: Fixtures generated without error at `test/v3_julia/data.zarr/`.

**Step 3: Verify fixtures can be read back**

Add a test to `test/v3_codecs.jl`:

```julia
@testset "Read Julia-generated v3 fixtures" begin
    fixture_path = joinpath(@__DIR__, "v3_julia", "data.zarr")
    if isdir(fixture_path)
        store = Zarr.DirectoryStore(fixture_path)
        # Read a simple 1d array
        z = zopen(store, path="1d.contiguous.raw.i2")
        @test z[:] == Int16[1, 2, 3, 4]

        # Read a chunked 2d array
        z2 = zopen(store, path="2d.chunked.i2")
        @test z2[:, :] == Int16[1 2; 3 4]
    else
        @warn "v3 fixtures not found at $fixture_path, skipping"
    end
end
```

**Step 4: Run tests**

Run: `julia --project -e 'using Test, Zarr; include("test/v3_codecs.jl")'`
Expected: Fixture read tests pass (if fixtures were generated).

**Step 5: Commit**

```bash
git add test/v3_julia.jl test/v3_codecs.jl
git commit -m "fix: update v3 test fixtures to use DirectoryStore + zarr_format=3"
```

---

### Task 8: Integration Tests and Cleanup

**Files:**
- Modify: `test/v3_codecs.jl` (comprehensive integration tests)
- Modify: `test/runtests.jl` (ensure v3 tests are included)

**Step 1: Add comprehensive v3 integration tests**

Add to `test/v3_codecs.jl`:

```julia
@testset "V3 Integration" begin
    @testset "zzeros with v3" begin
        z = zzeros(Float32, 10, 10; zarr_format=3, chunks=(5, 5), fill_value=Float32(0))
        @test size(z) == (10, 10)
        @test all(==(0.0f0), z[:, :])
    end

    @testset "V3 zopen round-trip with DirectoryStore" begin
        mktempdir() do dir
            path = joinpath(dir, "test.zarr")
            z = zcreate(Int64, 4, 4; path=path, zarr_format=3,
                chunks=(2, 2), fill_value=Int64(0))
            z[:, :] = reshape(Int64.(1:16), 4, 4)

            z2 = zopen(path)
            @test z2[:, :] == reshape(Int64.(1:16), 4, 4)
        end
    end

    @testset "V3 group with arrays" begin
        store = Zarr.DictStore()
        g = zgroup(store, "", Zarr.ZarrFormat(3))
        a = zcreate(Float64, g, "myarray", 10; zarr_format=3,
            chunks=(5,), fill_value=0.0)
        a[:] = Float64.(1:10)

        g2 = zopen(store)
        @test g2["myarray"][:] == Float64.(1:10)
    end
end
```

**Step 2: Run all tests**

Run: `julia --project -e 'using Pkg; Pkg.test()'`
Expected: All tests pass.

**Step 3: Commit**

```bash
git add test/v3_codecs.jl test/runtests.jl
git commit -m "test: add comprehensive v3 integration tests"
```
