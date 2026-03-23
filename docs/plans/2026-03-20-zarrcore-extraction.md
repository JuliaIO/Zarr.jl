# ZarrCore.jl Extraction Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Extract a minimal `ZarrCore.jl` package from `Zarr.jl` into `lib/ZarrCore/`, sufficient to read/write uncompressed Zarr v2 and v3 arrays, with Zarr.jl as a thin wrapper that registers compressors, codecs, and network stores.

**Architecture:** ZarrCore owns all abstract types, registries, core stores (DirectoryStore, DictStore, ConsolidatedStore), all pure-Julia filters, and the minimal V3 codecs (BytesCodec, TransposeCodec). Zarr.jl depends on ZarrCore and populates its registries with Blosc/Zlib/Zstd compressors, compression V3 codecs, and HTTP/GCS/ZIP stores. A `default_compressor()` function allows Zarr.jl to override the default from `NoCompressor()` to `BloscCompressor()`.

**Tech Stack:** Julia 1.10+, JSON, DiskArrays, OffsetArrays, DateTimes64

---

## Phase 1: ZarrCore Package Scaffold

### Task 1: Create ZarrCore directory and Project.toml

**Files:**
- Create: `lib/ZarrCore/Project.toml`

**Step 1: Create the file**

```toml
name = "ZarrCore"
uuid = "INSERT-GENERATED-UUID"
version = "0.1.0"

[deps]
JSON = "682c06a0-de6a-54ab-a142-c8b1cf79cde6"
DiskArrays = "3c3547ce-8d99-4f5e-a174-61eb10b00ae3"
OffsetArrays = "6fe1bfb0-de20-5000-8ca7-80f57d26f881"
DateTimes64 = "b342263e-b350-472a-b1a9-8dfd21b51589"
Dates = "ade2ca70-3891-5945-98fb-dc099432e06a"

[compat]
JSON = "0.21, 1"
DiskArrays = "0.4.2"
OffsetArrays = "0.11, 1.0"
DateTimes64 = "1"
julia = "1.10"
```

Generate a real UUID with: `julia -e 'using UUIDs; println(uuid4())'`

**Step 2: Verify directory structure**

Run: `ls lib/ZarrCore/`
Expected: `Project.toml`

**Step 3: Commit**

```bash
git add lib/ZarrCore/Project.toml
git commit -m "scaffold: create ZarrCore package with Project.toml"
```

---

### Task 2: Create ZarrCore module entry point (stub)

**Files:**
- Create: `lib/ZarrCore/src/ZarrCore.jl`

**Step 1: Create a minimal module that loads**

```julia
module ZarrCore

import JSON

struct ZarrFormat{V}
  version::Val{V}
end
Base.Int(v::ZarrFormat{V}) where V = V
@inline ZarrFormat(v::Int) = ZarrFormat(Val(v))
ZarrFormat(v::ZarrFormat) = v
const DV = ZarrFormat(Val(2))

abstract type AbstractCodecPipeline end

# Placeholder — will be populated in subsequent tasks
end # module
```

**Step 2: Verify it loads**

Run: `julia --project=lib/ZarrCore -e 'using Pkg; Pkg.instantiate(); using ZarrCore; println("OK")'`
Expected: `OK`

**Step 3: Commit**

```bash
git add lib/ZarrCore/src/ZarrCore.jl
git commit -m "scaffold: add ZarrCore module entry point"
```

---

## Phase 2: Core Types & Registries

### Task 3: Move chunkkeyencoding.jl to ZarrCore

**Files:**
- Create: `lib/ZarrCore/src/chunkkeyencoding.jl` (copy from `src/chunkkeyencoding.jl`)

This file has no external dependencies — it only references `ZarrFormat` which is defined in the module entry point. Copy it verbatim.

**Step 1: Copy the file**

```bash
cp src/chunkkeyencoding.jl lib/ZarrCore/src/chunkkeyencoding.jl
```

**Step 2: Add include to ZarrCore.jl**

In `lib/ZarrCore/src/ZarrCore.jl`, add after the `AbstractCodecPipeline` line:

```julia
include("chunkkeyencoding.jl")
```

**Step 3: Verify**

Run: `julia --project=lib/ZarrCore -e 'using ZarrCore; println(ZarrCore.ChunkKeyEncoding(".", false))'`
Expected: no error

**Step 4: Commit**

```bash
git add lib/ZarrCore/src/chunkkeyencoding.jl lib/ZarrCore/src/ZarrCore.jl
git commit -m "core: move chunkkeyencoding.jl to ZarrCore"
```

---

### Task 4: Move MaxLengthStrings.jl to ZarrCore

**Files:**
- Create: `lib/ZarrCore/src/MaxLengthStrings.jl` (copy from `src/MaxLengthStrings.jl`)

No external deps. Copy verbatim.

**Step 1: Copy**

```bash
cp src/MaxLengthStrings.jl lib/ZarrCore/src/MaxLengthStrings.jl
```

**Step 2: Verify**

Run: `julia --project=lib/ZarrCore -e 'include("lib/ZarrCore/src/MaxLengthStrings.jl"); using .MaxLengthStrings; println("OK")'`
Expected: `OK`

**Step 3: Commit**

```bash
git add lib/ZarrCore/src/MaxLengthStrings.jl
git commit -m "core: move MaxLengthStrings.jl to ZarrCore"
```

---

### Task 5: Create ZarrCore Compressors (abstract type + NoCompressor + registry)

**Files:**
- Create: `lib/ZarrCore/src/Compressors/Compressors.jl`

Extract the abstract type, registry, `NoCompressor`, and all generic fallback methods from `src/Compressors/Compressors.jl`. Do NOT include `blosc.jl`, `zlib.jl`, `zstd.jl`. Also add `default_compressor()` function.

**Step 1: Create the file**

```julia
import JSON # for JSON.lower

_reinterpret(::Type{T}, x::AbstractArray{S, 0}) where {T, S} = reinterpret(T, reshape(x, 1))
_reinterpret(::Type{T}, x::AbstractArray) where T = reinterpret(T, x)

"""
    abstract type Compressor

The abstract supertype for all Zarr compressors.

## Interface

All subtypes of `Compressor` SHALL implement the following methods:

- `zcompress(a, c::Compressor)`: compress the array `a` using the compressor `c`.
- `zuncompress(a, c::Compressor, T)`: uncompress the array `a` using the compressor `c`
  and return an array of type `T`.
- `JSON.lower(c::Compressor)`: return a JSON representation of the compressor `c`, which
  follows the Zarr specification for that compressor.
- `getCompressor(::Type{<:Compressor}, d::Dict)`: return a compressor object from a given
  dictionary `d` which contains the compressor's parameters according to the Zarr spec.

Subtypes of `Compressor` MAY also implement the following methods:

- `zcompress!(compressed, data, c::Compressor)`: compress the array `data` using the
  compressor `c` and store the result in the array `compressed`.
- `zuncompress!(data, compressed, c::Compressor)`: uncompress the array `compressed`
  using the compressor `c` and store the result in the array `data`.

Finally, an entry MUST be added to the `compressortypes` dictionary for each compressor type.
"""
abstract type Compressor end

const compressortypes = Dict{Union{String,Nothing}, Type{<: Compressor}}()

# Fallback definitions for the compressor interface
getCompressor(compdict::Dict) = haskey(compdict, "id") ?
    getCompressor(compressortypes[compdict["id"]], compdict) :
    getCompressor(compressortypes[compdict["name"]], compdict["configuration"])
getCompressor(::Nothing) = NoCompressor()

# Compression when no filter is given
zcompress!(compressed,data,c,::Nothing) = zcompress!(compressed,data,c)
zuncompress!(data,compressed,c,::Nothing) = zuncompress!(data,compressed,c)

# Fallback definition of mutating form of compress and uncompress
function zcompress!(compressed, data, c)
    empty!(compressed)
    append!(compressed,zcompress(data, c))
end
zuncompress!(data, compressed, c) = copyto!(data, zuncompress(compressed, c, eltype(data)))


# Function given a filter stack
function zcompress!(compressed, data, c, f)
    a2 = foldl(f, init=data) do anow, fnow
        zencode(anow,fnow)
    end
    zcompress!(compressed, a2, c)
end

function zuncompress!(data, compressed, c, f)
    data2 = zuncompress(compressed, c, desttype(last(f)))
    a2 = foldr(f, init = data2) do fnow, anow
        zdecode(anow, fnow)
    end
    copyto!(data, a2)
end

# NoCompressor
"""
    NoCompressor()

Creates an object that can be passed to ZArray constructors without compression.
"""
struct NoCompressor <: Compressor end

function zuncompress(a, ::NoCompressor, T)
  _reinterpret(T,a)
end

function zcompress(a, ::NoCompressor)
  _reinterpret(UInt8,a)
end

JSON.lower(::NoCompressor) = nothing

compressortypes[nothing] = NoCompressor

"""
    default_compressor()

Returns the default compressor used by `zcreate` and related functions.
ZarrCore returns `NoCompressor()`. Zarr.jl overrides this to `BloscCompressor()`.
"""
default_compressor() = NoCompressor()
```

**Step 2: Add include to ZarrCore.jl**

After the `include("chunkkeyencoding.jl")` line add:

```julia
include("Compressors/Compressors.jl")
```

**Step 3: Verify**

Run: `julia --project=lib/ZarrCore -e 'using ZarrCore; println(ZarrCore.NoCompressor())'`
Expected: `NoCompressor()`

**Step 4: Commit**

```bash
git add lib/ZarrCore/src/Compressors/Compressors.jl lib/ZarrCore/src/ZarrCore.jl
git commit -m "core: add Compressors base with NoCompressor and registry"
```

---

### Task 6: Create ZarrCore Codecs with registry-based V3 infrastructure

**Files:**
- Create: `lib/ZarrCore/src/Codecs/Codecs.jl`
- Create: `lib/ZarrCore/src/Codecs/V3/V3.jl`

The Codecs.jl is copied from `src/Codecs/Codecs.jl` with its `include` pointing to the new V3.

The V3.jl contains:
- `V3Codec{In,Out}` abstract type
- `v3_codec_parsers` registry (`Dict{String, Function}`)
- `codec_to_dict` generic function (for serialization)
- `codec_category` helper
- `BytesCodec` + `TransposeCodec` (structs, encode/decode, parse, serialize)
- `encoded_shape` generic function

**Step 1: Create `lib/ZarrCore/src/Codecs/Codecs.jl`**

```julia
module Codecs

using JSON: JSON

"""
    abstract type Codec

The abstract supertype for all Zarr codecs.
"""
abstract type Codec end

zencode(a, c::Codec) = error("Unimplemented")
zencode!(encoded, data, c::Codec) = error("Unimplemented")
zdecode(a, c::Codec, T::Type) = error("Unimplemented")
zdecode!(data, encoded, c::Codec) = error("Unimplemented")
JSON.lower(c::Codec) = error("Unimplemented")
getCodec(::Type{<:Codec}, d::Dict) = error("Unimplemented")

include("V3/V3.jl")

end
```

**Step 2: Create `lib/ZarrCore/src/Codecs/V3/V3.jl`**

```julia
module V3Codecs

import ..Codecs: zencode, zdecode, zencode!, zdecode!
using JSON: JSON

abstract type V3Codec{In,Out} end

"""Registry of V3 codec parsers: name -> (config_dict -> V3Codec instance)"""
const v3_codec_parsers = Dict{String, Function}()

"""Each V3Codec must implement this for JSON serialization."""
function codec_to_dict end

"""Classify a V3Codec by its phase in the pipeline."""
codec_category(::V3Codec{:array,:array}) = :array_array
codec_category(::V3Codec{:array,:bytes}) = :array_bytes
codec_category(::V3Codec{:bytes,:bytes}) = :bytes_bytes

"""Return the shape of the output of `codec_encode(codec, data)` given the input shape."""
encoded_shape(::V3Codec, sz::NTuple{N,Int}) where {N} = sz

# --- BytesCodec (array -> bytes) ---

struct BytesCodec <: V3Codec{:array, :bytes}
    endian::Symbol  # :little or :big
    function BytesCodec(endian::Symbol)
        endian ∈ (:little, :big) ||
            throw(ArgumentError("BytesCodec endian must be :little or :big, got :$endian"))
        new(endian)
    end
end
BytesCodec() = BytesCodec(:little)

const _SYSTEM_LITTLE_ENDIAN = Base.ENDIAN_BOM == 0x04030201
_needs_bswap(endian::Symbol) = (endian == :little) != _SYSTEM_LITTLE_ENDIAN

function codec_encode(c::BytesCodec, data::AbstractArray)
    if _needs_bswap(c.endian)
        return reinterpret(UInt8, bswap.(vec(data))) |> collect
    else
        return reinterpret(UInt8, vec(data)) |> collect
    end
end

function codec_decode(c::BytesCodec, encoded::Vector{UInt8}, ::Type{T}, shape::NTuple{N,Int}) where {T, N}
    arr = collect(reinterpret(T, encoded))
    if _needs_bswap(c.endian)
        arr = bswap.(arr)
    end
    return reshape(arr, shape)
end

codec_to_dict(c::BytesCodec) = Dict{String,Any}(
    "name" => "bytes",
    "configuration" => Dict{String,Any}("endian" => string(c.endian))
)

v3_codec_parsers["bytes"] = function(config)
    endian_str = get(config, "endian", "little")
    endian = endian_str == "little" ? :little :
             endian_str == "big"    ? :big    :
             throw(ArgumentError("Unknown endian value: \"$endian_str\""))
    BytesCodec(endian)
end

# --- TransposeCodec (array -> array) ---

struct TransposeCodec{N} <: V3Codec{:array, :array}
    order::NTuple{N, Int}  # permutation (1-based Julia indexing)
end

encoded_shape(c::TransposeCodec, sz::NTuple{N,Int}) where {N} = ntuple(i -> sz[c.order[i]], Val{N}())

function codec_encode(c::TransposeCodec, data::AbstractArray)
    return permutedims(data, c.order)
end

function codec_decode(c::TransposeCodec, encoded::AbstractArray)
    inv_order = Tuple(invperm(collect(c.order)))
    return permutedims(encoded, inv_order)
end

codec_to_dict(c::TransposeCodec) = Dict{String,Any}(
    "name" => "transpose",
    "configuration" => Dict{String,Any}("order" => collect(c.order .- 1))
)

# TransposeCodec parser is registered in metadata3.jl because it needs
# the shape context to parse string orders like "C"/"F".
# The registry entry for numeric orders is here:
# NOTE: the parser receives (config, shape_length) — see parse_v3_codec.

end
```

**Step 3: Add include to ZarrCore.jl**

After `include("Compressors/Compressors.jl")`:

```julia
include("Codecs/Codecs.jl")
```

**Step 4: Verify**

Run: `julia --project=lib/ZarrCore -e 'using ZarrCore; c = ZarrCore.Codecs.V3Codecs.BytesCodec(); println(c)'`
Expected: `BytesCodec(:little)`

**Step 5: Commit**

```bash
git add lib/ZarrCore/src/Codecs/Codecs.jl lib/ZarrCore/src/Codecs/V3/V3.jl lib/ZarrCore/src/ZarrCore.jl
git commit -m "core: add Codecs module with BytesCodec and TransposeCodec"
```

---

### Task 7: Move Filters to ZarrCore

**Files:**
- Create: `lib/ZarrCore/src/Filters/` (entire directory)

Copy the entire Filters directory verbatim. These files only depend on JSON (imported by parent module) and have no external package deps.

**Step 1: Copy all filter files**

```bash
cp -r src/Filters lib/ZarrCore/src/Filters
```

**Step 2: Add include to ZarrCore.jl**

After `include("Codecs/Codecs.jl")`:

```julia
include("Filters/Filters.jl")
```

**Step 3: Verify**

Run: `julia --project=lib/ZarrCore -e 'using ZarrCore; println(ZarrCore.filterdict)'`
Expected: prints a Dict with filter entries

**Step 4: Commit**

```bash
git add lib/ZarrCore/src/Filters/ lib/ZarrCore/src/ZarrCore.jl
git commit -m "core: move all Filters to ZarrCore"
```

---

## Phase 3: Metadata Layer

### Task 8: Move metadata.jl to ZarrCore

**Files:**
- Create: `lib/ZarrCore/src/metadata.jl`

Copy `src/metadata.jl` with these changes:

1. The `Metadata(A, chunks, zarr_format; compressor=BloscCompressor(), ...)` constructor — change default to `default_compressor()`.
2. The `Metadata(A, chunks, ::ZarrFormat{2}; compressor=BloscCompressor(), ...)` constructor — change default to `default_compressor()`.

These are the ONLY changes. Everything else stays the same.

**Step 1: Copy and modify**

```bash
cp src/metadata.jl lib/ZarrCore/src/metadata.jl
```

Then edit `lib/ZarrCore/src/metadata.jl`:

Replace both occurrences of `compressor::C=BloscCompressor()` with `compressor::C=default_compressor()`:

- Line ~153 (in `Metadata(A::AbstractArray{T,N}, chunks::NTuple{N,Int}, zarr_format=DV; ...)`):
  Change `compressor::C=BloscCompressor()` → `compressor::C=default_compressor()`

- Line ~175 (in `Metadata(A::AbstractArray{T,N}, chunks::NTuple{N,Int}, ::ZarrFormat{2}; ...)`):
  Change `compressor::C=BloscCompressor()` → `compressor::C=default_compressor()`

**Step 2: Add include to ZarrCore.jl**

After `include("chunkkeyencoding.jl")` and before `include("Compressors/Compressors.jl")`, add:

```julia
include("metadata.jl")
```

Wait — `metadata.jl` uses `BloscCompressor` and `getCompressor` which are defined in Compressors. And it uses `getfilters` from Filters. So the include order must be:

```julia
include("chunkkeyencoding.jl")
abstract type AbstractCodecPipeline end
include("Compressors/Compressors.jl")
include("Codecs/Codecs.jl")
include("Filters/Filters.jl")
include("metadata.jl")
```

Actually, `metadata.jl` only needs `Compressor` types for the constructor default and `getCompressor`/`getfilters` for parsing. Both are already included before metadata.jl. This matches the original `src/Zarr.jl` include order.

**Step 3: Verify**

Run: `julia --project=lib/ZarrCore -e 'using ZarrCore; println(ZarrCore.MetadataV2)'`
Expected: prints the type

**Step 4: Commit**

```bash
git add lib/ZarrCore/src/metadata.jl lib/ZarrCore/src/ZarrCore.jl
git commit -m "core: move metadata.jl to ZarrCore with default_compressor()"
```

---

### Task 9: Refactor and move metadata3.jl to ZarrCore

**Files:**
- Create: `lib/ZarrCore/src/metadata3.jl`

This is the biggest refactoring. The current `metadata3.jl` has:

1. **`Metadata3(d::AbstractDict, ...)`** — hardcodes codec parsing with if/elseif chain
2. **`lower3(md::MetadataV3)`** — hardcodes codec serialization with isa checks
3. **`MetadataV3{T2,N}` convenience constructor** — hardcodes compressor-to-codec mapping

Changes needed:

**A. Replace hardcoded codec parsing with registry lookup:**

The current code:
```julia
if codec_name == "transpose" ...
elseif codec_name == "bytes" ...
elseif codec_name == "gzip" ...
```

Becomes:
```julia
function parse_v3_codec(codec_name::String, config::Dict, shape_length::Int)
    # Special case: transpose needs shape_length for "C"/"F" string orders
    if codec_name == "transpose"
        return _parse_transpose(config, shape_length)
    end
    haskey(Codecs.V3Codecs.v3_codec_parsers, codec_name) ||
        throw(ArgumentError("Zarr.jl currently does not support the $codec_name codec"))
    return Codecs.V3Codecs.v3_codec_parsers[codec_name](config)
end
```

**B. Replace hardcoded serialization with `codec_to_dict` dispatch:**

The current `lower3` builds codec dicts inline with isa checks. Replace with:
```julia
for codec in p.array_array
    push!(codecs, Codecs.V3Codecs.codec_to_dict(codec))
end
```

**C. Replace hardcoded compressor-to-codec in convenience constructor:**

The `MetadataV3{T2,N}(...)` constructor maps compressors to V3 codecs. This constructor moves to Zarr.jl (not ZarrCore) since it references specific compressor types. ZarrCore keeps only the inner constructor and `Metadata3(d::AbstractDict, ...)`.

**Step 1: Create the refactored file**

Create `lib/ZarrCore/src/metadata3.jl` with this content:

```julia
"""
Prototype Zarr version 3 support
"""

const typemap3 = Dict{String, DataType}()
foreach([Bool, Int8, Int16, Int32, Int64, UInt8, UInt16, UInt32, UInt64, Float16, Float32, Float64]) do t
    typemap3[lowercase(string(t))] = t
end
typemap3["complex64"] = ComplexF32
typemap3["complex128"] = ComplexF64

function typestr3(t::Type)
    return lowercase(string(t))
end
function typestr3(::Type{NTuple{N,UInt8}}) where {N}
    return "r$(N*8)"
end

function typestr3(s::AbstractString, codecs=nothing)
    if !haskey(typemap3, s)
        if startswith(s, "r")
            num_bits = tryparse(Int, s[2:end])
            if isnothing(num_bits)
                throw(ArgumentError("$s is not a known type"))
            end
            if mod(num_bits, 8) == 0
                return NTuple{num_bits÷8,UInt8}
            else
                throw(ArgumentError("$s must describe a raw type with bit size that is a multiple of 8 bits"))
            end
        end
    end
    return typemap3[s]
end

function check_keys(d::AbstractDict, keys)
    for key in keys
        if !haskey(d, key)
            throw(ArgumentError("Zarr v3 metadata must have a key called $key"))
        end
    end
end

"""Metadata for Zarr version 3 arrays"""
struct MetadataV3{T,N,P<:AbstractCodecPipeline,E<:AbstractChunkKeyEncoding} <: AbstractMetadata{T,N,E}
    zarr_format::Int
    node_type::String
    shape::Base.RefValue{NTuple{N, Int}}
    chunks::NTuple{N, Int}
    dtype::String  # data_type in v3
    pipeline::P
    fill_value::Union{T, Nothing}
    chunk_key_encoding::E
    function MetadataV3{T2,N,P,E}(zarr_format, node_type, shape, chunks, dtype, pipeline, fill_value, chunk_key_encoding) where {T2,N,P,E}
        zarr_format == 3 || throw(ArgumentError("MetadataV3 only functions if zarr_format == 3"))
        any(<(0), shape) && throw(ArgumentError("Size must be positive"))
        any(<(1), chunks) && throw(ArgumentError("Chunk size must be >= 1 along each dimension"))
        new{T2,N,P,E}(zarr_format, node_type, Base.RefValue{NTuple{N,Int}}(shape), chunks, dtype, pipeline, fill_value, chunk_key_encoding)
    end
end
MetadataV3{T2,N,P}(args...) where {T2,N,P} = MetadataV3{T2,N,P,ChunkKeyEncoding}(args...)
zarr_format(::MetadataV3) = ZarrFormat(Val(3))

function Base.:(==)(m1::MetadataV3, m2::MetadataV3)
  m1.zarr_format == m2.zarr_format &&
  m1.node_type == m2.node_type &&
  m1.shape[] == m2.shape[] &&
  m1.chunks == m2.chunks &&
  m1.dtype == m2.dtype &&
  m1.fill_value == m2.fill_value &&
  m1.pipeline == m2.pipeline &&
  m1.chunk_key_encoding == m2.chunk_key_encoding
end

"""
Derive the storage order ('C' or 'F') from the codec pipeline of a MetadataV3.
"""
function get_order(md::MetadataV3)
    array_array = md.pipeline.array_array
    if length(array_array) == 0
        return 'C'
    end
    if length(array_array) > 1
        throw(ArgumentError(
            "Cannot determine storage order: pipeline has $(length(array_array)) " *
            "array->array codecs; composed permutations yield an indeterminate order"
        ))
    end
    codec = only(array_array)
    if !(codec isa Codecs.V3Codecs.TransposeCodec)
        throw(ArgumentError(
            "Cannot determine storage order: unrecognized array->array codec $(typeof(codec))"
        ))
    end
    N = ndims(md)
    c_perm  = ntuple(identity, N)
    f_perm  = ntuple(i -> N - i + 1, N)
    if codec.order == c_perm
        return 'C'
    elseif codec.order == f_perm
        return 'F'
    else
        throw(ArgumentError(
            "Cannot determine storage order: TransposeCodec permutation $(codec.order) " *
            "is neither C order $c_perm nor F order $f_perm"
        ))
    end
end
get_order(md::MetadataV2) = md.order

# --- Registry-based codec parsing ---

"""
Parse a single V3 codec entry from a zarr.json codecs array.
`shape_length` is needed for transpose codec string order parsing ("C"/"F").
"""
function parse_v3_codec(codec_name::String, config::Dict, shape_length::Int)
    # Transpose needs shape_length for "C"/"F" string parsing
    if codec_name == "transpose"
        return _parse_transpose_codec(config, shape_length)
    end
    haskey(Codecs.V3Codecs.v3_codec_parsers, codec_name) ||
        throw(ArgumentError("Zarr.jl currently does not support the $codec_name codec"))
    return Codecs.V3Codecs.v3_codec_parsers[codec_name](config)
end

function _parse_transpose_codec(config::Dict, shape_length::Int)
    _order = config["order"]
    if _order isa AbstractString
        n = shape_length
        if _order == "C"
            @warn "Transpose codec dimension order of C is deprecated"
            perm = ntuple(identity, n)
        elseif _order == "F"
            @warn "Transpose codec dimension order of F is deprecated"
            perm = ntuple(i -> n - i + 1, n)
        else
            throw(ArgumentError("Unknown transpose order string: $_order"))
        end
    else
        perm = Tuple(Int.(_order) .+ 1)
    end
    return Codecs.V3Codecs.TransposeCodec(perm)
end


function Metadata3(d::AbstractDict, fill_as_missing)
    check_keys(d, ("zarr_format", "node_type"))

    zarr_format = d["zarr_format"]::Int
    node_type = d["node_type"]::String
    if node_type ∉ ("group", "array")
        throw(ArgumentError("Unknown node_type of $node_type"))
    end
    zarr_format == 3 || throw(ArgumentError("Metadata3 only functions if zarr_format == 3"))

    # Groups
    if node_type == "group"
        for key in keys(d)
            if key ∉ ("zarr_format", "node_type", "attributes")
                throw(ArgumentError("Zarr v3 group metadata cannot have a key called $key"))
            end
        end
        group_pipeline = V3Pipeline((), Codecs.V3Codecs.BytesCodec(), ())
        return MetadataV3{Int,0,typeof(group_pipeline),ChunkKeyEncoding}(zarr_format, node_type, (), (), "", group_pipeline, 0, ChunkKeyEncoding('/', true))
    end

    # Array keys
    mandatory_keys = [
        "zarr_format", "node_type", "shape", "data_type",
        "chunk_grid", "chunk_key_encoding", "fill_value", "codecs",
    ]
    optional_keys = ["attributes", "storage_transformers", "dimension_names"]
    check_keys(d, mandatory_keys)
    for key in keys(d)
        if key ∉ mandatory_keys && key ∉ optional_keys
            throw(ArgumentError("Zarr v3 metadata cannot have a key called $key"))
        end
    end

    shape = Int.(d["shape"])
    data_type = d["data_type"]::String

    chunk_grid = d["chunk_grid"]
    if chunk_grid["name"] == "regular"
        chunks = Int.(chunk_grid["configuration"]["chunk_shape"])
        if length(shape) != length(chunks)
            throw(ArgumentError("Shape has rank $(length(shape)) which does not match the chunk_shape rank of $(length(chunks))"))
        end
    else
        throw(ArgumentError("Unknown chunk_grid of name, $(chunk_grid["name"])"))
    end

    # Build V3Pipeline from codec chain using registry
    array_array_codecs = []
    array_bytes_codec = nothing
    bytes_bytes_codecs = []

    for codec in d["codecs"]
        codec_name = codec["name"]
        config = get(codec, "configuration", Dict{String,Any}())
        parsed = parse_v3_codec(codec_name, config, length(shape))
        cat = Codecs.V3Codecs.codec_category(parsed)
        if cat == :array_array
            push!(array_array_codecs, parsed)
        elseif cat == :array_bytes
            array_bytes_codec = parsed
        elseif cat == :bytes_bytes
            push!(bytes_bytes_codecs, parsed)
        end
    end

    isnothing(array_bytes_codec) && throw(ArgumentError("V3 codec chain must contain a 'bytes' codec"))
    pipeline = V3Pipeline(Tuple(array_array_codecs), array_bytes_codec, Tuple(bytes_bytes_codecs))

    T = typestr3(data_type)
    N = length(shape)
    fv = fill_value_decoding(d["fill_value"], T)::T
    TU = (fv === nothing || !fill_as_missing) ? T : Union{T,Missing}

    chunk_key_encoding = parse_chunk_key_encoding(d["chunk_key_encoding"])
    E = typeof(chunk_key_encoding)

    MetadataV3{TU, N, typeof(pipeline), E}(
        zarr_format, node_type,
        NTuple{N, Int}(shape) |> reverse,
        NTuple{N, Int}(chunks) |> reverse,
        data_type, pipeline, fv, chunk_key_encoding,
    )
end

"Construct MetadataV3 based on your data (minimal — no compressor mapping)"
function Metadata3(A::AbstractArray{T, N}, chunks::NTuple{N, Int};
        node_type::String="array",
        fill_value::Union{T, Nothing}=nothing,
        order::Char='C',
        endian::Symbol=:little,
        fill_as_missing = false,
        dimension_separator::Char = '/',
        bytes_bytes_codecs::Tuple=()
    ) where {T, N}
    @warn("Zarr v3 support is experimental")
    T2 = (fill_value === nothing || !fill_as_missing) ? T : Union{T,Missing}
    if fill_value === nothing
        fill_value = zero(T)
    end
    array_array_codecs = if order == 'F'
        (Codecs.V3Codecs.TransposeCodec(ntuple(i -> N - i + 1, N)),)
    else
        ()
    end
    array_bytes_codec = Codecs.V3Codecs.BytesCodec(endian)
    pipeline = V3Pipeline(array_array_codecs, array_bytes_codec, bytes_bytes_codecs)
    chunk_key_encoding = ChunkKeyEncoding(dimension_separator, true)
    E = typeof(chunk_key_encoding)
    return MetadataV3{T2,N,typeof(pipeline),E}(
        3, node_type, size(A), chunks, typestr3(eltype(A)),
        pipeline, fill_value, chunk_key_encoding
    )
end

# --- Registry-based serialization ---

function lower3(md::MetadataV3{T}) where T
    chunk_grid = Dict{String,Any}(
        "name" => "regular",
        "configuration" => Dict{String,Any}(
            "chunk_shape" => md.chunks |> reverse
        )
    )
    chunk_key_encoding = lower_chunk_key_encoding(md.chunk_key_encoding)

    codecs = Dict{String,Any}[]
    p = md.pipeline

    for codec in p.array_array
        push!(codecs, Codecs.V3Codecs.codec_to_dict(codec))
    end
    push!(codecs, Codecs.V3Codecs.codec_to_dict(p.array_bytes))
    for codec in p.bytes_bytes
        push!(codecs, Codecs.V3Codecs.codec_to_dict(codec))
    end

    Dict{String, Any}(
        "zarr_format" => Int(md.zarr_format),
        "node_type" => md.node_type,
        "shape" => md.shape[] |> reverse,
        "data_type" => typestr3(T),
        "chunk_grid" => chunk_grid,
        "chunk_key_encoding" => chunk_key_encoding,
        "fill_value" => fill_value_encoding(md.fill_value),
        "codecs" => codecs
    )
end

function Metadata(A::AbstractArray{T,N}, chunks::NTuple{N,Int}, ::ZarrFormat{3};
        node_type::String="array",
        compressor::C=default_compressor(),
        fill_value::Union{T, Nothing}=nothing,
        order::Char='C',
        endian::Symbol=:little,
        filters::F=nothing,
        fill_as_missing = false,
        chunk_key_encoding::E=ChunkKeyEncoding('/', true)
    ) where {T, N, C, F, E}
    # Map compressor to bytes_bytes_codecs using compressor_to_v3_codecs registry
    bytes_bytes = compressor_to_v3_bytes_codecs(compressor)
    return Metadata3(A, chunks;
        node_type=node_type,
        fill_value=fill_value,
        order=order,
        endian=endian,
        fill_as_missing=fill_as_missing,
        dimension_separator=chunk_key_encoding.sep,
        bytes_bytes_codecs=bytes_bytes
    )
end

# V3 constructor from Dict
function Metadata(d::AbstractDict, fill_as_missing, ::ZarrFormat{3})
    return Metadata3(d, fill_as_missing)
end

function JSON.lower(md::MetadataV3)
    return lower3(md)
end

"""
    compressor_to_v3_bytes_codecs(c::Compressor) -> Tuple

Convert a v2 Compressor to a tuple of V3 bytes->bytes codecs.
ZarrCore handles NoCompressor. Zarr.jl adds methods for BloscCompressor, etc.
"""
compressor_to_v3_bytes_codecs(::NoCompressor) = ()
compressor_to_v3_bytes_codecs(c::Compressor) = throw(ArgumentError(
    "Unsupported compressor type for v3: $(typeof(c)). Load Zarr.jl for compression support."
))
```

**Step 2: Add include to ZarrCore.jl**

After `include("metadata.jl")`:

```julia
include("metadata3.jl")
```

**Step 3: Verify**

Run: `julia --project=lib/ZarrCore -e 'using ZarrCore; println(ZarrCore.MetadataV3)'`
Expected: prints the type

**Step 4: Commit**

```bash
git add lib/ZarrCore/src/metadata3.jl lib/ZarrCore/src/ZarrCore.jl
git commit -m "core: add refactored metadata3.jl with registry-based codec parsing"
```

---

### Task 10: Move pipeline.jl to ZarrCore

**Files:**
- Create: `lib/ZarrCore/src/pipeline.jl`

Copy `src/pipeline.jl` verbatim. It references `Codecs.V3Codecs.codec_encode` and `codec_decode` which exist in ZarrCore's V3Codecs module.

**Step 1: Copy**

```bash
cp src/pipeline.jl lib/ZarrCore/src/pipeline.jl
```

**Step 2: Add include to ZarrCore.jl**

After `include("metadata3.jl")`:

```julia
include("pipeline.jl")
```

Note: in the original `src/Zarr.jl`, `pipeline.jl` is included after `ZArray.jl`. However, `pipeline.jl` only depends on types already defined (V3Pipeline, Codecs.V3Codecs, Compressors). `ZArray.jl` calls `pipeline_encode`/`pipeline_decode!` which are defined in `pipeline.jl`. So `pipeline.jl` must come before `ZArray.jl`. Check that this doesn't break any circular dependencies — it shouldn't because `pipeline.jl` doesn't reference ZArray.

Actually wait, looking at the original include order in `src/Zarr.jl`:
```julia
include("ZArray.jl")
include("pipeline.jl")
```

But `ZArray.jl` calls `pipeline_encode`/`pipeline_decode!`/`get_pipeline`. These are defined in `pipeline.jl`. How does this work? Because Julia resolves function calls at runtime, not at include time. The struct `V2Pipeline`/`V3Pipeline` are used as types in `V2Pipeline{C,F}` etc. But `V3Pipeline` is defined... where? Let me check.

`V3Pipeline` is defined in `pipeline.jl` (line 30). But `MetadataV3` references `pipeline::P` where `P<:AbstractCodecPipeline`, and `metadata3.jl` constructs `V3Pipeline(...)`. So `pipeline.jl` must be included BEFORE `metadata3.jl`.

Looking at the original order: `metadata3.jl` is included at line 18, `pipeline.jl` at line 24. But `metadata3.jl` uses `V3Pipeline(...)`. How? Because in the original code, `V3Pipeline` is defined in `pipeline.jl` which is included AFTER `metadata3.jl`. This works because the `Metadata3` function that constructs `V3Pipeline` isn't called until runtime.

So the include order doesn't matter for function bodies, only for type definitions and struct references. Since `AbstractCodecPipeline` is defined before `metadata3.jl`, and `V3Pipeline` is only used in function bodies (not in struct fields — `MetadataV3` uses `P<:AbstractCodecPipeline`), this works.

For ZarrCore, include `pipeline.jl` BEFORE `ZArray.jl` to match the logical dependency order. But technically it works either way.

**Step 3: Verify**

Run: `julia --project=lib/ZarrCore -e 'using ZarrCore; p = ZarrCore.V3Pipeline((), ZarrCore.Codecs.V3Codecs.BytesCodec(), ()); println(p)'`
Expected: prints the pipeline

**Step 4: Commit**

```bash
git add lib/ZarrCore/src/pipeline.jl lib/ZarrCore/src/ZarrCore.jl
git commit -m "core: move pipeline.jl to ZarrCore"
```

---

## Phase 4: Storage & Array

### Task 11: Move core Storage to ZarrCore

**Files:**
- Create: `lib/ZarrCore/src/Storage/Storage.jl`
- Create: `lib/ZarrCore/src/Storage/directorystore.jl`
- Create: `lib/ZarrCore/src/Storage/dictstore.jl`
- Create: `lib/ZarrCore/src/Storage/consolidated.jl`

Copy `Storage.jl` but remove the includes for `gcstore.jl`, `http.jl`, `zipstore.jl`. Also remove the `S3Store` stub (move it to Zarr.jl) and the S3 regex registration.

**Step 1: Copy store implementation files verbatim**

```bash
cp src/Storage/directorystore.jl lib/ZarrCore/src/Storage/directorystore.jl
cp src/Storage/dictstore.jl lib/ZarrCore/src/Storage/dictstore.jl
cp src/Storage/consolidated.jl lib/ZarrCore/src/Storage/consolidated.jl
```

**Step 2: Create modified `lib/ZarrCore/src/Storage/Storage.jl`**

Copy `src/Storage/Storage.jl` with these changes:

1. Remove the `S3Store` struct definition and constructor (lines 36-43)
2. Remove `push!(storageregexlist, r"^s3://" => S3Store)` (line 283)
3. Remove `include("gcstore.jl")`, `include("http.jl")`, `include("zipstore.jl")` (lines 288-291)

The remaining includes are: `directorystore.jl`, `dictstore.jl`, `consolidated.jl`.

**Step 3: Add include to ZarrCore.jl**

After `include("pipeline.jl")`:

```julia
include("Storage/Storage.jl")
```

**Step 4: Verify**

Run: `julia --project=lib/ZarrCore -e 'using ZarrCore; d = ZarrCore.DictStore(); println(d)'`
Expected: `Dictionary Storage`

**Step 5: Commit**

```bash
git add lib/ZarrCore/src/Storage/ lib/ZarrCore/src/ZarrCore.jl
git commit -m "core: move Storage with DirectoryStore, DictStore, ConsolidatedStore"
```

---

### Task 12: Move ZArray.jl to ZarrCore

**Files:**
- Create: `lib/ZarrCore/src/ZArray.jl`

Copy `src/ZArray.jl` with these changes:

1. `zcreate(::Type{T}, storage::AbstractStore, ...)` — change `compressor=BloscCompressor()` → `compressor=default_compressor()`

That's the only change needed. The rest (imports, types, functions) stays the same.

**Step 1: Copy and modify**

```bash
cp src/ZArray.jl lib/ZarrCore/src/ZArray.jl
```

Edit `lib/ZarrCore/src/ZArray.jl`:
- Change `compressor=BloscCompressor()` to `compressor=default_compressor()` in the `zcreate(::Type{T},storage::AbstractStore, ...)` function (around line 349).

**Step 2: Add include to ZarrCore.jl**

After `include("Storage/Storage.jl")`:

```julia
include("ZArray.jl")
```

**Step 3: Verify**

Run: `julia --project=lib/ZarrCore -e 'using ZarrCore; z = ZarrCore.zzeros(Int, 4, 4); println(z)'`
Expected: `ZArray{Int64} of size 4 x 4`

**Step 4: Commit**

```bash
git add lib/ZarrCore/src/ZArray.jl lib/ZarrCore/src/ZarrCore.jl
git commit -m "core: move ZArray.jl to ZarrCore with default_compressor()"
```

---

### Task 13: Move ZGroup.jl to ZarrCore

**Files:**
- Create: `lib/ZarrCore/src/ZGroup.jl`

Copy `src/ZGroup.jl` with these changes:

1. Remove `HTTP.serve(...)` line (line 201) — move to Zarr.jl
2. Remove `writezip(...)` line (line 202) — move to Zarr.jl

These are the only changes. `consolidate_metadata` stays (it only depends on Store and JSON).

**Step 1: Copy and modify**

```bash
cp src/ZGroup.jl lib/ZarrCore/src/ZGroup.jl
```

Edit `lib/ZarrCore/src/ZGroup.jl`:
- Remove the line: `HTTP.serve(s::Union{ZArray,ZGroup}, args...; kwargs...) = HTTP.serve(s.storage, s.path, args...; kwargs...)`
- Remove the line: `writezip(io::IO, s::Union{ZArray,ZGroup}; kwargs...) = writezip(io, s.storage, s.path; kwargs...)`

**Step 2: Add include to ZarrCore.jl**

After `include("ZArray.jl")`:

```julia
include("ZGroup.jl")
```

**Step 3: Verify**

Run: `julia --project=lib/ZarrCore -e 'using ZarrCore; g = ZarrCore.zgroup(ZarrCore.DictStore(), "", ZarrCore.ZarrFormat(2)); println(g)'`
Expected: `ZarrGroup at Dictionary Storage and path `

**Step 4: Commit**

```bash
git add lib/ZarrCore/src/ZGroup.jl lib/ZarrCore/src/ZarrCore.jl
git commit -m "core: move ZGroup.jl to ZarrCore (without HTTP.serve, writezip)"
```

---

## Phase 5: Module Entry Points & Wiring

### Task 14: Finalize ZarrCore.jl module entry with exports

**Files:**
- Modify: `lib/ZarrCore/src/ZarrCore.jl`

Write the complete module entry point with all includes and exports.

**Step 1: Write the final ZarrCore.jl**

```julia
module ZarrCore

import JSON

struct ZarrFormat{V}
  version::Val{V}
end
Base.Int(v::ZarrFormat{V}) where V = V
@inline ZarrFormat(v::Int) = ZarrFormat(Val(v))
ZarrFormat(v::ZarrFormat) = v
#Default Zarr Version
const DV = ZarrFormat(Val(2))

include("chunkkeyencoding.jl")
abstract type AbstractCodecPipeline end
include("Compressors/Compressors.jl")
include("Codecs/Codecs.jl")
include("Filters/Filters.jl")
include("metadata.jl")
include("metadata3.jl")
include("pipeline.jl")
include("Storage/Storage.jl")
include("ZArray.jl")
include("ZGroup.jl")

export ZArray, ZGroup, zopen, zzeros, zcreate, storagesize, storageratio,
  zinfo, DirectoryStore, DictStore, ConsolidatedStore, zgroup

end # module
```

**Step 2: Verify full load**

Run: `julia --project=lib/ZarrCore -e 'using ZarrCore; z = zzeros(Int, 4, 4); println(z); println(z[1:2, 1:2])'`
Expected: creates a ZArray and reads zeros back

**Step 3: Commit**

```bash
git add lib/ZarrCore/src/ZarrCore.jl
git commit -m "core: finalize ZarrCore module with all exports"
```

---

### Task 15: Rewrite Zarr.jl as wrapper

**Files:**
- Modify: `src/Zarr.jl`
- Keep: `src/Compressors/blosc.jl`, `src/Compressors/zlib.jl`, `src/Compressors/zstd.jl`
- Keep: `src/Storage/gcstore.jl`, `src/Storage/http.jl`, `src/Storage/zipstore.jl`

The new `src/Zarr.jl` imports ZarrCore, re-exports its names, includes the compression and network store files, and registers everything into ZarrCore's registries.

**Step 1: Rewrite `src/Zarr.jl`**

```julia
module Zarr

using ZarrCore
import JSON
import Blosc

# Re-export all ZarrCore public names
using ZarrCore: ZarrCore,
    # Types
    ZArray, ZGroup, AbstractStore, DirectoryStore, DictStore, ConsolidatedStore,
    AbstractMetadata, MetadataV2, MetadataV3,
    AbstractCodecPipeline, V2Pipeline, V3Pipeline,
    Compressor, NoCompressor,
    Filter,
    ZarrFormat,
    AbstractChunkKeyEncoding, ChunkKeyEncoding, SuffixChunkKeyEncoding,
    ASCIIChar,
    # Functions
    zcreate, zopen, zzeros, zgroup,
    storagesize, storageratio, zinfo, zname,
    pipeline_encode, pipeline_decode!, get_pipeline, get_order,
    Metadata, Metadata3,
    typestr, typestr3,
    fill_value_encoding, fill_value_decoding,
    getCompressor, compressortypes,
    getfilters, filterdict,
    zencode, zdecode,
    citostring,
    default_sep, default_prefix,
    storageregexlist, storefromstring,
    chunkindices,
    consolidate_metadata,
    # Pipeline/Codec internals used by tests
    _reinterpret,
    # Constants
    DV, DS, DS2, DS3

# Re-export the same symbols as before
export ZArray, ZGroup, zopen, zzeros, zcreate, storagesize, storageratio,
  zinfo, DirectoryStore, DictStore, ConsolidatedStore, zgroup

# Bring in the MaxLengthStrings submodule
using ZarrCore: MaxLengthStrings
using .MaxLengthStrings: MaxLengthString

# Include compressor implementations (they register into ZarrCore.compressortypes)
include("Compressors/blosc.jl")
include("Compressors/zlib.jl")
include("Compressors/zstd.jl")

# Override default compressor
ZarrCore.default_compressor() = BloscCompressor()

# Override compressor_to_v3_bytes_codecs for specific compressor types
function ZarrCore.compressor_to_v3_bytes_codecs(c::BloscCompressor)
    T_base = UInt8  # will be overridden by actual element type at call site
    (ZarrCore.Codecs.V3Codecs.BloscV3Codec(c.cname, c.clevel, c.shuffle, c.blocksize, sizeof(T_base)),)
end
function ZarrCore.compressor_to_v3_bytes_codecs(c::ZlibCompressor)
    level = c.config.level == -1 ? 6 : c.config.level
    (ZarrCore.Codecs.V3Codecs.GzipV3Codec(level),)
end
function ZarrCore.compressor_to_v3_bytes_codecs(c::ZstdCompressor)
    (ZarrCore.Codecs.V3Codecs.ZstdV3Codec(c.config.compressionLevel),)
end

# Include V3 compression codec implementations
include("Codecs/V3/compression_codecs.jl")

# Include network/archive storage backends
include("Storage/gcstore.jl")
include("Storage/http.jl")
include("Storage/zipstore.jl")

# Register S3Store stub
struct S3Store <: AbstractStore
    bucket::String
    aws::Any
end
function S3Store(args...)
    error("AWSS3 must be loaded to use S3Store. Try `using AWSS3`.")
end

# Register store URL resolvers
push!(storageregexlist, r"^s3://" => S3Store)

# HTTP.serve and writezip for ZArray/ZGroup
HTTP.serve(s::Union{ZArray,ZGroup}, args...; kwargs...) = HTTP.serve(s.storage, s.path, args...; kwargs...)
writezip(io::IO, s::Union{ZArray,ZGroup}; kwargs...) = writezip(io, s.storage, s.path; kwargs...)

export S3Store, GCStore

end # module
```

**Important:** The compressor files (`blosc.jl`, `zlib.jl`, `zstd.jl`) need to be updated to reference `ZarrCore.compressortypes` instead of `Zarr.compressortypes` since the registry lives in ZarrCore. However, since `Zarr` uses `ZarrCore` and imports `compressortypes`, the line `Zarr.compressortypes["blosc"] = BloscCompressor` will still work (it references the same dict object). No change needed in the compressor files.

Wait — actually, the compressor files do `Zarr.compressortypes["blosc"] = BloscCompressor`. Since `compressortypes` is imported into `Zarr` from `ZarrCore`, `Zarr.compressortypes` IS `ZarrCore.compressortypes`. So this works without changes.

**Step 2: Verify**

Run: `julia --project -e 'using Zarr; z = zzeros(Int, 4, 4); println(z)'`
Expected: `ZArray{Int64} of size 4 x 4` (with Blosc compression as before)

**Step 3: Commit**

```bash
git add src/Zarr.jl
git commit -m "wrapper: rewrite Zarr.jl to use ZarrCore with re-exports"
```

---

### Task 16: Create V3 compression codec file for Zarr.jl

**Files:**
- Create: `src/Codecs/V3/compression_codecs.jl`

Extract the compression-related V3 codecs from the original `src/Codecs/V3/V3.jl` into a new file. This includes: `GzipV3Codec`, `BloscV3Codec`, `ZstdV3Codec`, `CRC32cV3Codec`, and their `codec_encode`/`codec_decode`/`codec_to_dict` methods, plus the `CRC32cCodec` helper, `ShardingCodec`, and all sharding infrastructure.

Each codec must also register its parser in `ZarrCore.Codecs.V3Codecs.v3_codec_parsers`.

**Step 1: Create `src/Codecs/V3/compression_codecs.jl`**

```julia
# V3 compression codecs that depend on Blosc, ChunkCodecLibZlib, ChunkCodecLibZstd, CRC32c
# These register into ZarrCore's V3 codec registry.

using CRC32c: CRC32c
using ChunkCodecLibZlib: GzipCodec as LibZGzipCodec, GzipEncodeOptions
using ChunkCodecCore: encode as cc_encode, decode as cc_decode

import ZarrCore.Codecs.V3Codecs: V3Codec, v3_codec_parsers, codec_to_dict,
    codec_encode, codec_decode, zencode, zdecode, zencode!, zdecode!

# --- CRC32c internal codec (used by CRC32cV3Codec) ---

struct CRC32cCodec
end

function crc32c_stream!(output::IO, input::IO; buffer = Vector{UInt8}(undef, 1024*32))
    hash::UInt32 = 0x00000000
    while(bytesavailable(input) > 0)
        sized_buffer = @view(buffer[1:min(length(buffer), bytesavailable(input))])
        read!(input, sized_buffer)
        write(output, sized_buffer)
        hash = CRC32c.crc32c(sized_buffer, hash)
    end
    return hash
end

function zencode!(encoded::Vector{UInt8}, data::Vector{UInt8}, c::CRC32cCodec)
    output = IOBuffer(encoded, read=false, write=true)
    input = IOBuffer(data, read=true, write=false)
    zencode!(output, input, c)
    return take!(output)
end
function zencode!(output::IO, input::IO, c::CRC32cCodec)
    hash = crc32c_stream!(output, input)
    write(output, hash)
    return output
end
function zdecode!(encoded::Vector{UInt8}, data::Vector{UInt8}, c::CRC32cCodec)
    output = IOBuffer(encoded, read=false, write=true)
    input = IOBuffer(data, read=true, write=true)
    zdecode!(output, input, c)
    return take!(output)
end
function zdecode!(output::IOBuffer, input::IOBuffer, c::CRC32cCodec)
    input_vec = take!(input)
    truncated_input = IOBuffer(@view(input_vec[1:end-4]); read=true, write=false)
    hash = crc32c_stream!(output, truncated_input)
    if input_vec[end-3:end] != reinterpret(UInt8, [hash])
        throw(IOError("CRC32c hash does not match"))
    end
    return output
end

# --- GzipV3Codec ---

struct GzipV3Codec <: V3Codec{:bytes, :bytes}
    level::Int
end
GzipV3Codec() = GzipV3Codec(6)

function codec_encode(c::GzipV3Codec, data::Vector{UInt8})
    opts = GzipEncodeOptions(; level=c.level)
    return cc_encode(opts, data)
end
function codec_decode(c::GzipV3Codec, encoded::Vector{UInt8})
    return cc_decode(LibZGzipCodec(), encoded)
end
codec_to_dict(c::GzipV3Codec) = Dict{String,Any}(
    "name" => "gzip",
    "configuration" => Dict{String,Any}("level" => c.level)
)

v3_codec_parsers["gzip"] = function(config)
    level = get(config, "level", 6)
    GzipV3Codec(level)
end

# --- BloscV3Codec ---

struct BloscV3Codec <: V3Codec{:bytes, :bytes}
    cname::String
    clevel::Int
    shuffle::Int
    blocksize::Int
    typesize::Int
end
BloscV3Codec() = BloscV3Codec("lz4", 5, 1, 0, 4)

function codec_encode(c::BloscV3Codec, data::Vector{UInt8})
    comp = Zarr.BloscCompressor(blocksize=c.blocksize, clevel=c.clevel, cname=c.cname, shuffle=c.shuffle)
    return ZarrCore.zcompress(data, comp)
end
function codec_decode(c::BloscV3Codec, encoded::Vector{UInt8})
    comp = Zarr.BloscCompressor(blocksize=c.blocksize, clevel=c.clevel, cname=c.cname, shuffle=c.shuffle)
    return collect(ZarrCore.zuncompress(encoded, comp, UInt8))
end
codec_to_dict(c::BloscV3Codec) = Dict{String,Any}(
    "name" => "blosc",
    "configuration" => Dict{String,Any}(
        "cname" => c.cname,
        "clevel" => c.clevel,
        "shuffle" => c.shuffle == 0 ? "noshuffle" :
                     c.shuffle == 1 ? "shuffle" :
                     c.shuffle == 2 ? "bitshuffle" :
                     throw(ArgumentError("Unknown shuffle integer: $(c.shuffle)")),
        "blocksize" => c.blocksize,
        "typesize" => c.typesize
    )
)

v3_codec_parsers["blosc"] = function(config)
    cname = get(config, "cname", "lz4")
    clevel = get(config, "clevel", 5)
    shuffle_val = get(config, "shuffle", "noshuffle")
    shuffle_int = shuffle_val isa Integer ? shuffle_val :
                  shuffle_val == "noshuffle" ? 0 :
                  shuffle_val == "shuffle" ? 1 :
                  shuffle_val == "bitshuffle" ? 2 :
                  throw(ArgumentError("Unknown shuffle: \"$shuffle_val\"."))
    blocksize = get(config, "blocksize", 0)
    typesize = get(config, "typesize", 4)
    BloscV3Codec(string(cname), clevel, shuffle_int, blocksize, typesize)
end

# --- ZstdV3Codec ---

struct ZstdV3Codec <: V3Codec{:bytes, :bytes}
    level::Int
end
ZstdV3Codec() = ZstdV3Codec(3)

function codec_encode(c::ZstdV3Codec, data::Vector{UInt8})
    comp = Zarr.ZstdCompressor(level=c.level)
    return ZarrCore.zcompress(data, comp)
end
function codec_decode(c::ZstdV3Codec, encoded::Vector{UInt8})
    comp = Zarr.ZstdCompressor(level=c.level)
    return collect(ZarrCore.zuncompress(encoded, comp, UInt8))
end
codec_to_dict(c::ZstdV3Codec) = Dict{String,Any}(
    "name" => "zstd",
    "configuration" => Dict{String,Any}("level" => c.level)
)

v3_codec_parsers["zstd"] = function(config)
    level = get(config, "level", 3)
    ZstdV3Codec(level)
end

# --- CRC32cV3Codec ---

struct CRC32cV3Codec <: V3Codec{:bytes, :bytes}
end

function codec_encode(c::CRC32cV3Codec, data::Vector{UInt8})
    out = UInt8[]
    return zencode!(out, data, CRC32cCodec())
end
function codec_decode(c::CRC32cV3Codec, encoded::Vector{UInt8})
    out = UInt8[]
    return zdecode!(out, encoded, CRC32cCodec())
end
codec_to_dict(::CRC32cV3Codec) = Dict{String,Any}("name" => "crc32c")

v3_codec_parsers["crc32c"] = function(config)
    CRC32cV3Codec()
end

# --- ShardingCodec (stub — throws at parse time) ---

v3_codec_parsers["sharding_indexed"] = function(config)
    throw(ArgumentError("Zarr.jl currently does not support the sharding_indexed codec"))
end
```

**Note:** The full `ShardingCodec` struct and all its encode/decode/index infrastructure from the original V3.jl should also be moved here. For brevity, the plan shows the registry stub. Copy the full ShardingCodec code (lines 106-539 of the original `src/Codecs/V3/V3.jl`) into this file.

**Step 2: Verify**

Run: `julia --project -e 'using Zarr; println(haskey(ZarrCore.Codecs.V3Codecs.v3_codec_parsers, "blosc"))'`
Expected: `true`

**Step 3: Commit**

```bash
git add src/Codecs/V3/compression_codecs.jl
git commit -m "wrapper: add V3 compression codecs with registry entries"
```

---

### Task 17: Update Project.toml files

**Files:**
- Modify: `Project.toml` (top-level Zarr.jl)
- Modify: `lib/ZarrCore/Project.toml` (if UUID not yet set)

**Step 1: Add ZarrCore as a dependency to Zarr.jl**

Add to `Project.toml` `[deps]` section:
```toml
ZarrCore = "<UUID-from-task-1>"
```

Add to `[sources]` (Julia 1.11+ for path deps, or use `[extras]` + dev approach):
```toml
[sources.ZarrCore]
path = "lib/ZarrCore"
```

For Julia < 1.11 compatibility, you may need to use `Pkg.develop(path="lib/ZarrCore")` instead. Check the Julia version requirements.

**Step 2: Remove dependencies that moved to ZarrCore**

From Zarr.jl's `Project.toml`, the deps that are ONLY used by ZarrCore files should be removed. However, some deps are used by both. Keep all deps for now to avoid breakage; optimize later.

Actually — since Zarr.jl `using ZarrCore` and ZarrCore has its own deps, Zarr.jl only needs to declare deps that IT directly uses (not transitive deps from ZarrCore). But for safety during initial migration, keep all deps. Can be cleaned up in a follow-up.

**Step 3: Verify**

Run: `julia --project -e 'using Pkg; Pkg.instantiate(); using Zarr; println("OK")'`
Expected: `OK`

**Step 4: Commit**

```bash
git add Project.toml lib/ZarrCore/Project.toml
git commit -m "deps: wire ZarrCore as dependency of Zarr.jl"
```

---

### Task 18: Clean up original src/ files

**Files:**
- Remove: Files that were moved to ZarrCore (they're now dead code in `src/`)

After Zarr.jl is rewritten as a wrapper, the old files in `src/` that were moved to ZarrCore are no longer included. Remove them to avoid confusion:

```
src/chunkkeyencoding.jl      → moved to lib/ZarrCore/src/
src/MaxLengthStrings.jl       → moved to lib/ZarrCore/src/
src/metadata.jl               → moved to lib/ZarrCore/src/
src/metadata3.jl              → moved to lib/ZarrCore/src/
src/pipeline.jl               → moved to lib/ZarrCore/src/
src/ZArray.jl                 → moved to lib/ZarrCore/src/
src/ZGroup.jl                 → moved to lib/ZarrCore/src/
src/Filters/                  → moved to lib/ZarrCore/src/
src/Storage/Storage.jl        → moved to lib/ZarrCore/src/
src/Storage/directorystore.jl → moved to lib/ZarrCore/src/
src/Storage/dictstore.jl      → moved to lib/ZarrCore/src/
src/Storage/consolidated.jl   → moved to lib/ZarrCore/src/
src/Compressors/Compressors.jl → split; core moved to lib/ZarrCore/src/
src/Codecs/Codecs.jl          → split; core moved to lib/ZarrCore/src/
src/Codecs/V3/V3.jl           → split; core moved to lib/ZarrCore/src/
```

**Step 1: Use git rm to remove moved files**

```bash
git rm src/chunkkeyencoding.jl src/MaxLengthStrings.jl src/metadata.jl src/metadata3.jl src/pipeline.jl src/ZArray.jl src/ZGroup.jl
git rm -r src/Filters/
git rm src/Storage/Storage.jl src/Storage/directorystore.jl src/Storage/dictstore.jl src/Storage/consolidated.jl
git rm src/Compressors/Compressors.jl src/Codecs/Codecs.jl src/Codecs/V3/V3.jl
```

**Step 2: Verify Zarr.jl still loads**

Run: `julia --project -e 'using Zarr; println("OK")'`
Expected: `OK`

**Step 3: Commit**

```bash
git add -A
git commit -m "cleanup: remove files that moved to ZarrCore"
```

---

## Phase 6: Tests & Verification

### Task 19: Update test Project.toml

**Files:**
- Modify: `test/Project.toml`

**Step 1: Add ZarrCore as a test dependency**

Add to `test/Project.toml` `[deps]`:
```toml
ZarrCore = "<UUID>"
```

Add source path:
```toml
[sources.ZarrCore]
path = "../lib/ZarrCore"
```

**Step 2: Commit**

```bash
git add test/Project.toml
git commit -m "test: add ZarrCore to test dependencies"
```

---

### Task 20: Run the existing test suite

**Step 1: Instantiate and run**

Run: `julia --project -e 'using Pkg; Pkg.test()'`

Expected: All existing tests pass. If any fail, investigate:

- **`Zarr.BloscCompressor` not found**: Ensure the re-export in `src/Zarr.jl` includes `BloscCompressor`
- **`Zarr.compressortypes` not found**: Ensure `compressortypes` is imported from ZarrCore
- **`Codecs.V3Codecs.XxxCodec` not found**: Ensure all V3 codec types are accessible through the module path
- **Codec parsing fails**: Ensure all V3 codec parsers are registered in `v3_codec_parsers`

Fix any failures before proceeding.

**Step 2: Commit any fixes**

```bash
git add -A
git commit -m "fix: resolve test failures from ZarrCore extraction"
```

---

### Task 21: Add ZarrCore-only smoke test

**Files:**
- Create: `lib/ZarrCore/test/runtests.jl`
- Create: `lib/ZarrCore/test/Project.toml`

**Step 1: Create `lib/ZarrCore/test/Project.toml`**

```toml
[deps]
Test = "8dfed614-e22c-5e08-85e1-65c5234f0b40"
JSON = "682c06a0-de6a-54ab-a142-c8b1cf79cde6"
ZarrCore = "<UUID>"

[sources.ZarrCore]
path = ".."
```

**Step 2: Create `lib/ZarrCore/test/runtests.jl`**

```julia
using Test
using ZarrCore
using JSON

@testset "ZarrCore" begin
    @testset "NoCompressor roundtrip" begin
        z = zzeros(Int64, 4, 4)
        @test z isa ZArray
        @test size(z) == (4, 4)
        @test z[1,1] == 0
        z[2,3] = 42
        @test z[2,3] == 42
    end

    @testset "DirectoryStore" begin
        p = mktempdir()
        z = zcreate(Float64, 10, 10; path=p, chunks=(5,5))
        z[:] = reshape(1.0:100.0, 10, 10)
        z2 = zopen(p)
        @test z2[1,1] == 1.0
        @test z2[10,10] == 100.0
    end

    @testset "DictStore" begin
        z = zcreate(Int32, 6, 6; chunks=(3,3))
        z[:] = ones(Int32, 6, 6)
        @test z[1,1] == 1
    end

    @testset "V3 uncompressed roundtrip" begin
        z = zcreate(Float32, 8, 8;
            zarr_format=3,
            chunks=(4,4),
            compressor=ZarrCore.NoCompressor())
        z[:] = reshape(Float32.(1:64), 8, 8)
        z2 = zopen(z.storage, path=z.path)
        @test z2[1,1] == 1.0f0
        @test z2[8,8] == 64.0f0
    end

    @testset "default_compressor is NoCompressor" begin
        @test ZarrCore.default_compressor() isa ZarrCore.NoCompressor
    end
end
```

**Step 3: Run ZarrCore tests**

Run: `julia --project=lib/ZarrCore -e 'using Pkg; Pkg.test()'`
Expected: All pass

**Step 4: Commit**

```bash
git add lib/ZarrCore/test/
git commit -m "test: add ZarrCore smoke tests"
```

---

### Task 22: Update CLAUDE.md

**Files:**
- Modify: `CLAUDE.md`

Add information about the ZarrCore package structure to the Architecture section. Add test commands for ZarrCore.

**Step 1: Add ZarrCore section**

Add to Build & Test Commands:
```bash
# Run ZarrCore tests only
julia --project=lib/ZarrCore -e 'using Pkg; Pkg.test()'
```

Add to Architecture section:
```
### Package Structure
- `lib/ZarrCore/` — Minimal core package with types, registries, NoCompressor, BytesCodec, TransposeCodec, DirectoryStore, DictStore, ConsolidatedStore, all pure-Julia filters
- `src/` (Zarr.jl) — Wrapper that depends on ZarrCore, adds Blosc/Zlib/Zstd compressors, V3 compression codecs, HTTP/GCS/S3/ZIP stores
- ZarrCore's `default_compressor()` returns `NoCompressor()`; Zarr.jl overrides it to `BloscCompressor()`
- V3 codecs use `v3_codec_parsers` registry for extensible parsing; Zarr.jl registers compression codec parsers
```

**Step 2: Commit**

```bash
git add CLAUDE.md
git commit -m "docs: update CLAUDE.md with ZarrCore package structure"
```

---

## Important Notes for Implementation

### Module Path References

In tests and existing code, `Zarr.SomeType` must still work. Since `Zarr.jl` does `using ZarrCore: SomeType`, the name is available as `Zarr.SomeType`. But for internal module paths like `Zarr.Codecs.V3Codecs.BloscV3Codec`, these need careful handling:

- Core V3 types (`BytesCodec`, `TransposeCodec`) are at `ZarrCore.Codecs.V3Codecs.BytesCodec`
- Compression V3 types (`BloscV3Codec`, etc.) are defined in Zarr.jl but registered into `ZarrCore.Codecs.V3Codecs.v3_codec_parsers`
- Tests that reference `Zarr.Codecs.V3Codecs.XxxCodec` will need updates to use either the Zarr-local name or the ZarrCore path

### The Compressor File Imports

The existing `src/Compressors/blosc.jl` has `import Blosc` and `Zarr.compressortypes["blosc"] = BloscCompressor`. Since `Zarr` module now imports `compressortypes` from ZarrCore, `Zarr.compressortypes` refers to the same dict. This should work without changes to the compressor files.

### S3 Extension

The `ext/ZarrAWSS3Ext.jl` extension references `Zarr.S3Store`, `Zarr.AbstractStore`, etc. Since `S3Store` is now defined in `Zarr.jl` (not ZarrCore) and `AbstractStore` is re-exported from ZarrCore, the extension should work. Verify by checking the extension file.

### Iterative Debugging

This refactoring touches many files. Expect some iteration in Task 20. Common issues:
- Missing re-exports in `Zarr.jl`
- Module path mismatches (`Zarr.X` vs `ZarrCore.X`)
- Function signatures referencing types not yet imported
- `using` vs `import` confusion (Julia is strict about these)

The debugging approach: run `using Zarr`, fix the first error, repeat until it loads. Then run the test suite.
