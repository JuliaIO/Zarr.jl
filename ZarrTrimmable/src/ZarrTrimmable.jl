"""
    ZarrTrimmable

A trim-safe, closed-set front end over `ZarrCore`'s statically typed
`zopen(T, Val(N), store, path; compressor = C)` / `zcreate` entry points.

Two layers, both written so that a juliac `--trim=safe` build can reach them:

  * [`@zarr_reader`](@ref) — the code generator. You declare a *closed set* of
    element types, ranks, codecs and stores; the macro emits a codec-pool and a
    store-pool struct that forward the `ZarrCore` compressor and storage
    interfaces with explicit `isa` chains, plus `ZarrCore.zopen` /
    `ZarrCore.zcreate` methods over that set. Nothing dispatches on the open
    world, so nothing widens to `Any`.
  * `ArrayMeta` and friends — an `Any`-free algebraic-data-type mirror of the
    Zarr v2 `.zarray` document, for reading, describing and re-writing array
    metadata without ever forming an abstract value.

The design rule everywhere below: **no open-world dispatch**. Element types,
compressors and fill values are each a `Union` of a fixed set of immutable
concrete structs, branched on with `isa` chains inside ordinary functions, and
every JSON write goes through a function barrier so the `NamedTuple` handed to
`JSON.print` is always concrete.

This package deliberately does not depend on any codec package: codec types
enter a reader only through the `codecs =` pool of [`@zarr_reader`](@ref), and
are admitted by their `ZarrCore.codec_id` method.
"""
module ZarrTrimmable

using ZarrCore, JSON

using ZarrCore: CompressorJSON, FillValueJSON, ZarrayJSON,
                parse_zarray, FV_NULL, FV_FLOAT, FV_INT, FV_STRING, FV_BOOL

export @zarr_reader

# ---------------------------------------------------------------------------
# DType -- closed sum type over the supported element types
# ---------------------------------------------------------------------------

"Singleton member of [`DType`](@ref) standing for `Float64` (`\"<f8\"`)."
struct DFloat64 end

"Singleton member of [`DType`](@ref) standing for `Float32` (`\"<f4\"`)."
struct DFloat32 end

"Singleton member of [`DType`](@ref) standing for `Int32` (`\"<i4\"`)."
struct DInt32 end

"Singleton member of [`DType`](@ref) standing for `Int64` (`\"<i8\"`)."
struct DInt64 end

"Singleton member of [`DType`](@ref) standing for `UInt8` (`\"|u1\"`)."
struct DUInt8 end

"""
    DType

Closed sum type over the element types this package understands:
`Union{DFloat64,DFloat32,DInt32,DInt64,DUInt8}`. Being a `Union` of singleton
structs rather than an abstract type is what keeps
[`dtype_from_typestr`](@ref) and its consumers `Any`-free.
"""
const DType = Union{DFloat64,DFloat32,DInt32,DInt64,DUInt8}

"""
    julia_type(d::DType)::Type

The Julia element type a [`DType`](@ref) stands for, e.g.
`julia_type(DFloat64()) === Float64`.
"""
julia_type(::DFloat64) = Float64
julia_type(::DFloat32) = Float32
julia_type(::DInt32)   = Int32
julia_type(::DInt64)   = Int64
julia_type(::DUInt8)   = UInt8

"""
    typestr(d::DType)::String

The numpy typestr a [`DType`](@ref) is stored under in `.zarray`, e.g.
`typestr(DFloat64()) == "<f8"`. Inverse of [`dtype_from_typestr`](@ref).
"""
# `ZarrCore.typestr(T)` is evaluated on a compile-time-constant `T` in each
# branch, so each method body folds to a literal string.
typestr(::DFloat64) = ZarrCore.typestr(Float64)
typestr(::DFloat32) = ZarrCore.typestr(Float32)
typestr(::DInt32)   = ZarrCore.typestr(Int32)
typestr(::DInt64)   = ZarrCore.typestr(Int64)
typestr(::DUInt8)   = ZarrCore.typestr(UInt8)

"""
    dtype_from_typestr(s::String)::DType

Map a numpy typestr as it appears in `.zarray` onto the closed [`DType`](@ref)
set. Throws `ArgumentError` for anything outside the set.
"""
function dtype_from_typestr(s::String)::DType
    if s == "<f8"
        return DFloat64()
    elseif s == "<f4"
        return DFloat32()
    elseif s == "<i4"
        return DInt32()
    elseif s == "<i8"
        return DInt64()
    elseif s == "|u1" || s == "<u1"
        return DUInt8()
    else
        throw(ArgumentError(string("unsupported dtype \"", s, "\"")))
    end
end

# Human-readable Julia type name, used by `describe`. Written out per branch so
# that `string(::Type)` never appears on a non-constant argument.
dtype_name(::DFloat64) = "Float64"
dtype_name(::DFloat32) = "Float32"
dtype_name(::DInt32)   = "Int32"
dtype_name(::DInt64)   = "Int64"
dtype_name(::DUInt8)   = "UInt8"

# ---------------------------------------------------------------------------
# CompressorSpec -- closed sum type over the supported compressors
# ---------------------------------------------------------------------------
#
# These are *pure data*: they describe what `.zarray` says, and carry no
# dependency on the packages that implement the codecs. Turning one into a
# working compressor is the caller's job (and needs their codec package).

"Member of [`CompressorSpec`](@ref) for an uncompressed array (`\"compressor\":null`)."
struct NoComp end

"""
    Zstd(level::Int)

Member of [`CompressorSpec`](@ref) describing a stored `{"id":"zstd",...}`
compressor. Pure metadata; it does not compress anything.
"""
struct Zstd
    level::Int
end

"""
    Zlib(level::Int)

Member of [`CompressorSpec`](@ref) describing a stored `{"id":"zlib",...}`
(or `"gzip"`) compressor. Pure metadata; it does not compress anything.
"""
struct Zlib
    level::Int
end

"""
    Blosc(cname::String, clevel::Int, shuffle::Int, blocksize::Int)

Member of [`CompressorSpec`](@ref) describing a stored `{"id":"blosc",...}`
compressor. Pure metadata; it does not compress anything.
"""
struct Blosc
    cname::String
    clevel::Int
    shuffle::Int
    blocksize::Int
end

"""
    CompressorSpec

Closed sum type over the v2 compressors this package can parse and re-serialise:
`Union{NoComp,Zstd,Zlib,Blosc}`. The members are data-only mirrors of the
`.zarray` `"compressor"` object — no codec package is needed to read or write
them.
"""
const CompressorSpec = Union{NoComp,Zstd,Zlib,Blosc}

"""
    compressor_id(c::CompressorSpec)::String

The numcodecs `"id"` a [`CompressorSpec`](@ref) is stored under, or `""` for
[`NoComp`](@ref) (stored as JSON `null`). The value-level counterpart of
`ZarrCore.codec_id`, which maps a compressor *type* to the same string.
"""
compressor_id(::NoComp) = ""
compressor_id(::Zstd)   = "zstd"
compressor_id(::Zlib)   = "zlib"
compressor_id(::Blosc)  = "blosc"

"""
    compressor_from_json(c::Union{Nothing,ZarrCore.CompressorJSON})::CompressorSpec

Branch on the numcodecs `id` and fill missing keys with each codec's own
defaults. `nothing` (JSON `null`) becomes [`NoComp`](@ref). Throws
`ArgumentError` on an unknown `id`.
"""
function compressor_from_json(c::Union{Nothing,CompressorJSON})::CompressorSpec
    if c === nothing
        return NoComp()
    end
    cc = c::CompressorJSON
    id = cc.id
    if id == "zstd"
        lvl = 0
        if cc.level !== nothing
            lvl = cc.level::Int
        end
        return Zstd(lvl)
    elseif id == "zlib" || id == "gzip"
        lvl = -1
        if cc.level !== nothing
            lvl = cc.level::Int
        elseif cc.clevel !== nothing
            lvl = cc.clevel::Int
        end
        return Zlib(lvl)
    elseif id == "blosc"
        cname = "lz4"
        if cc.cname !== nothing
            cname = cc.cname::String
        end
        clevel = 5
        if cc.clevel !== nothing
            clevel = cc.clevel::Int
        end
        shuffle = 1
        if cc.shuffle !== nothing
            shuffle = cc.shuffle::Int
        end
        blocksize = 0
        if cc.blocksize !== nothing
            blocksize = cc.blocksize::Int
        end
        return Blosc(cname, clevel, shuffle, blocksize)
    else
        throw(ArgumentError(string("unsupported compressor id \"", id, "\"")))
    end
end

# ---------------------------------------------------------------------------
# FillValue -- closed sum type over the v2 fill_value encodings
# ---------------------------------------------------------------------------

"Member of [`FillValue`](@ref) for `\"fill_value\":null` — the array has no fill value."
struct FillNone end

"""
    FillNumber(value::Float64)

Member of [`FillValue`](@ref) for a JSON floating-point `fill_value`, including
the `"Infinity"` / `"-Infinity"` spellings.
"""
struct FillNumber
    value::Float64
end

"Member of [`FillValue`](@ref) for `\"fill_value\":\"NaN\"`."
struct FillNaN end

"""
    FillInt(value::Int64)

Member of [`FillValue`](@ref) for a JSON integer `fill_value`. JSON `true` /
`false` are also mapped here, as `FillInt(1)` / `FillInt(0)`.
"""
struct FillInt
    value::Int64
end

"""
    FillValue

Closed sum type over the Zarr v2 `fill_value` encodings:
`Union{FillNone,FillNumber,FillNaN,FillInt}`. Splitting `NaN` out of
`FillNumber` keeps [`write_meta`](@ref)'s branch on the JSON spelling free of
floating-point predicates on an abstract value.
"""
const FillValue = Union{FillNone,FillNumber,FillNaN,FillInt}

"""
    fill_from_json(v::ZarrCore.FillValueJSON)::FillValue

`null` -> `FillNone()`, a JSON integer -> `FillInt`, a JSON float -> `FillNumber`,
`"NaN"` -> `FillNaN()`, `"Infinity"` / `"-Infinity"` -> `FillNumber(±Inf)`,
`true` / `false` -> `FillInt(1)` / `FillInt(0)`.
"""
function fill_from_json(v::FillValueJSON)::FillValue
    k = v.kind
    if k == FV_NULL
        return FillNone()
    elseif k == FV_INT
        return FillInt(v.int)
    elseif k == FV_FLOAT
        x = v.float
        if isnan(x)
            return FillNaN()
        else
            return FillNumber(x)
        end
    elseif k == FV_BOOL
        if v.bool
            return FillInt(Int64(1))
        else
            return FillInt(Int64(0))
        end
    elseif k == FV_STRING
        s = v.str
        if s == "NaN"
            return FillNaN()
        elseif s == "Infinity"
            return FillNumber(Inf)
        elseif s == "-Infinity"
            return FillNumber(-Inf)
        else
            throw(ArgumentError(string("unsupported string fill_value \"", s, "\"")))
        end
    else
        throw(ArgumentError("unsupported fill_value kind"))
    end
end

# ---------------------------------------------------------------------------
# ArrayMeta
# ---------------------------------------------------------------------------

"""
    ArrayMeta(dtype, shape, chunks, compressor, fill_value, order, dimension_separator)
    ArrayMeta(z::ZarrCore.ZarrayJSON)

Fully concrete, `Any`-free mirror of a Zarr v2 `.zarray`, with a
[`DType`](@ref), a [`CompressorSpec`](@ref) and a [`FillValue`](@ref) in place
of the three open-ended JSON fields.

`shape` and `chunks` are in **Julia (column-major, fastest-first) order**, i.e.
reversed relative to what is stored on disk; [`write_meta`](@ref) reverses them
back. The second form validates a parsed document: it requires
`zarr_format == 2`, matching `shape`/`chunks` lengths and no filters.

Read one with [`read_meta`](@ref) or [`read_meta_path`](@ref); write it back
with [`write_meta`](@ref) / [`write_meta_string`](@ref); summarise it with
[`describe`](@ref).
"""
struct ArrayMeta
    dtype::DType
    shape::Vector{Int}
    chunks::Vector{Int}
    compressor::CompressorSpec
    fill_value::FillValue
    order::Char
    dimension_separator::Char
end

"""
    ndims(m::ArrayMeta)::Int

The rank of the array `m` describes, i.e. `length(m.shape)`.
"""
Base.ndims(m::ArrayMeta) = length(m.shape)

# Both branches must return the same concrete type: a ternary over
# `Union{Nothing,String}` widens to `Any` under `--trim`.
function _sep(ds::Union{Nothing,String})
    if ds === nothing
        return '.'
    else
        return only(ds::String)
    end
end

function _reversed(v::Vector{Int})
    n = length(v)
    out = Vector{Int}(undef, n)
    for i in 1:n
        @inbounds out[i] = v[n - i + 1]
    end
    return out
end

function ArrayMeta(z::ZarrayJSON)
    z.zarr_format == 2 || throw(ArgumentError("not a Zarr v2 array (zarr_format is not 2)"))
    length(z.shape) == length(z.chunks) ||
        throw(ArgumentError("shape and chunks have different lengths"))
    z.filters === nothing ||
        throw(ArgumentError("filters are not supported by ZarrTrimmable"))
    return ArrayMeta(
        dtype_from_typestr(z.dtype),
        _reversed(z.shape),
        _reversed(z.chunks),
        compressor_from_json(z.compressor),
        fill_from_json(z.fill_value),
        only(z.order),
        _sep(z.dimension_separator),
    )
end

"""
    read_meta_path(store_path::String)::ArrayMeta

Read `<store_path>/.zarray` (or `store_path` itself if it already names the
`.zarray` file) and parse it into an [`ArrayMeta`](@ref).
"""
function read_meta_path(store_path::String)
    p = store_path
    if isdir(p)
        p = joinpath(p, ".zarray")
    end
    io = Base.open(p, "r")
    s = read(io, String)
    close(io)
    return ArrayMeta(parse_zarray(s))
end

"""
    read_meta(doc::Union{AbstractString,AbstractVector{UInt8}})::ArrayMeta

Parse a `.zarray` document into an [`ArrayMeta`](@ref).

`doc::Vector{UInt8}` is always `.zarray` bytes. `doc::String` is a store
directory, a `.zarray` file path, or the `.zarray` text itself — whichever it
turns out to be at run time (a leading `{` means "text"); the path forms
delegate to [`read_meta_path`](@ref).
"""
read_meta(b::Vector{UInt8}) = ArrayMeta(parse_zarray(b))

function read_meta(s::String)
    if !isempty(s) && s[1] == '{'
        return ArrayMeta(parse_zarray(s))
    else
        return read_meta_path(s)
    end
end

# Convenience forwards for non-`String`/`Vector{UInt8}` inputs. These are not
# on the trim-safe path; the concrete methods above are.
read_meta(s::AbstractString) = read_meta(String(s))
read_meta(b::AbstractVector{UInt8}) = read_meta(Vector{UInt8}(b))

# ---------------------------------------------------------------------------
# Writing -- byte-compatible with ZarrCore's `print_metadata(io, ::MetadataV2, false)`
# ---------------------------------------------------------------------------
#
# `_lower_v2` (ZarrCore/src/metadata.jl) fixes the key order as
#   zarr_format, node_type, shape (reversed), chunks (reversed), dtype,
#   compressor, fill_value, order, filters, dimension_separator
# and each codec package supplies `JSON.lower` for the compressor value.
#
# `compressor` and `fill_value` are the only runtime-varying *types* in that
# NamedTuple, so two nested function barriers turn one dynamic write into
# 4 x 4 statically typed `JSON.print` call sites.

"""
    write_meta(io::IO, m::ArrayMeta)

Serialise `m` as a Zarr v2 `.zarray` document, byte-compatible with what
`ZarrCore.print_metadata(io, ::MetadataV2, false)` writes for the same array.
`m.shape` and `m.chunks` are reversed back into on-disk order.
"""
function write_meta(io::IO, m::ArrayMeta)
    c = m.compressor
    if c isa NoComp
        _write_fv(io, m, nothing)
    elseif c isa Zstd
        _write_fv(io, m, (; id = "zstd", level = c.level))
    elseif c isa Zlib
        _write_fv(io, m, (; id = "zlib", level = c.level))
    else
        b = c::Blosc
        _write_fv(io, m, (; id = "blosc", cname = b.cname, clevel = b.clevel,
                            shuffle = b.shuffle, blocksize = b.blocksize))
    end
    return nothing
end

# Second barrier: specialised on the concrete compressor NamedTuple type, then
# on the concrete encoded fill-value type. `ZarrCore.fill_value_encoding`
# maps NaN/Inf onto their string spellings.
function _write_fv(io::IO, m::ArrayMeta, comp)
    fv = m.fill_value
    if fv isa FillNone
        _write_nt(io, m, comp, nothing)
    elseif fv isa FillInt
        _write_nt(io, m, comp, fv.value)
    elseif fv isa FillNaN
        _write_nt(io, m, comp, "NaN")
    else
        x = (fv::FillNumber).value
        if isnan(x)
            _write_nt(io, m, comp, "NaN")
        elseif isinf(x)
            if x > 0
                _write_nt(io, m, comp, "Infinity")
            else
                _write_nt(io, m, comp, "-Infinity")
            end
        else
            _write_nt(io, m, comp, x)
        end
    end
    return nothing
end

function _write_nt(io::IO, m::ArrayMeta, comp, fill_value)
    nt = (;
        zarr_format = 2,
        node_type = "array",
        shape = _reversed(m.shape),
        chunks = _reversed(m.chunks),
        dtype = typestr(m.dtype),
        compressor = comp,
        fill_value = fill_value,
        order = m.order,
        filters = nothing,
        dimension_separator = m.dimension_separator,
    )
    JSON.print(io, nt)
    return nothing
end

"""
    write_meta_string(m::ArrayMeta)::String

[`write_meta`](@ref) into an `IOBuffer` and return the resulting text.
"""
function write_meta_string(m::ArrayMeta)
    io = IOBuffer()
    write_meta(io, m)
    return String(take!(io))
end

# ---------------------------------------------------------------------------
# describe
# ---------------------------------------------------------------------------

# Everything here prints into an `IOBuffer` instead of calling the varargs
# `string(...)`: `string` with more than a handful of arguments lowers to
# `print_to_string(xs...)`, whose `for x in xs` loop over a long heterogeneous
# tuple is not unrolled, so `Base._str_sizehint(::Any)` / `print(::IOBuffer, ::Any)`
# become unresolved calls under `--trim=safe`.

function _print_dims(io::IO, v::Vector{Int})
    for i in 1:length(v)
        if i > 1
            print(io, 'x')
        end
        print(io, v[i])
    end
    return nothing
end

function _print_compressor(io::IO, c::CompressorSpec)
    if c isa NoComp
        print(io, "none")
    elseif c isa Zstd
        print(io, "zstd(level=")
        print(io, (c::Zstd).level)
        print(io, ')')
    elseif c isa Zlib
        print(io, "zlib(level=")
        print(io, (c::Zlib).level)
        print(io, ')')
    else
        b = c::Blosc
        print(io, "blosc(cname=")
        print(io, b.cname)
        print(io, ",clevel=")
        print(io, b.clevel)
        print(io, ",shuffle=")
        print(io, b.shuffle)
        print(io, ",blocksize=")
        print(io, b.blocksize)
        print(io, ')')
    end
    return nothing
end

function _print_fill(io::IO, f::FillValue)
    if f isa FillNone
        print(io, "none")
    elseif f isa FillNaN
        print(io, "NaN")
    elseif f isa FillInt
        print(io, (f::FillInt).value)
    else
        print(io, (f::FillNumber).value)
    end
    return nothing
end

"""
    describe(m::ArrayMeta)::String

A one-line human-readable summary of an [`ArrayMeta`](@ref), e.g.
`"Float64 4x6 chunks=2x3 ndims=2 compressor=zstd(level=3) fill=0.0 order=C sep=."`.
`Any`-free and trim-clean.
"""
function describe(m::ArrayMeta)
    io = IOBuffer()
    print(io, dtype_name(m.dtype))
    print(io, ' ')
    _print_dims(io, m.shape)
    print(io, " chunks=")
    _print_dims(io, m.chunks)
    print(io, " ndims=")
    print(io, ndims(m))
    print(io, " compressor=")
    _print_compressor(io, m.compressor)
    print(io, " fill=")
    _print_fill(io, m.fill_value)
    print(io, " order=")
    print(io, m.order)
    print(io, " sep=")
    print(io, m.dimension_separator)
    return String(take!(io))
end

include("pools.jl")

@static if VERSION >= v"1.11"
    include("public_names_trimmable.jl")
end

end # module
