abstract type AbstractChunkKeyEncoding end

struct ChunkKeyEncoding <: AbstractChunkKeyEncoding
    sep::Char
    prefix::Bool
end

# Default Zarr v2 separator
const DS2 = '.'
# Default Zarr v3 separator
const DS3 = '/'

default_sep(::ZarrFormat{2}) = DS2
default_sep(::ZarrFormat{3}) = DS3
default_sep(v::Int) = default_sep(ZarrFormat(v))
default_prefix(::ZarrFormat{2}) = false
default_prefix(::ZarrFormat{3}) = true
const DS = default_sep(DV)

@inline function citostring(e::ChunkKeyEncoding, i::CartesianIndex)
  if e.prefix
    "c$(e.sep)" * join(reverse((i - oneunit(i)).I), e.sep)
  else
    join(reverse((i - oneunit(i)).I), e.sep)
  end
end
@inline citostring(e::ChunkKeyEncoding, ::CartesianIndex{0}) = e.prefix ? "c$(e.sep)0" : "0"

"""
    SuffixChunkKeyEncoding{E<:AbstractChunkKeyEncoding}

Chunk key encoding that appends a user-defined suffix string to the key
produced by a base chunk key encoding. The primary use case is adding file
extensions (e.g. `.tiff`, `.shard.zip`) so that individual chunk files are
directly usable by other software without Zarr-specific tooling.

Per the zarr-extensions `suffix` chunk-key-encoding proposal.
"""
struct SuffixChunkKeyEncoding{E<:AbstractChunkKeyEncoding} <: AbstractChunkKeyEncoding
    suffix::String
    base_encoding::E
end

SuffixChunkKeyEncoding(suffix::String; sep::Char='/', prefix::Bool=true) =
    SuffixChunkKeyEncoding(suffix, ChunkKeyEncoding(sep, prefix))

@inline citostring(e::SuffixChunkKeyEncoding, i::CartesianIndex) =
    citostring(e.base_encoding, i) * e.suffix

"""
A single `{...}` expression of a [`TemplateChunkKeyEncoding`](@ref) format string.
`index === nothing` denotes the catch-all `{*}`; otherwise `index` is the 0-based
(or, when negative, from-the-end) position into the Zarr (row-major) chunk coordinate.
`width` is the minimum zero-padded width (`0` for no padding).
"""
struct TemplateExpr
    index::Union{Int,Nothing}
    width::Int
end

"""
    TemplateChunkKeyEncoding(format::AbstractString, separator=nothing)

Chunk key encoding following the zarr-extensions
[`template`](https://github.com/zarr-developers/zarr-extensions/tree/main/chunk-key-encodings/template)
specification. The `format` string contains `{}` expressions that are replaced by
values of the chunk coordinate, which is expressed in Zarr (row-major) order:

- `{0}`, `{1}`, … index the coordinate from the start; `{-1}`, `{-2}`, … from the end
- `{*}` inserts all coordinate values not used by another expression, joined by `separator`
  (required if `{*}` is present, at most one `{*}` is allowed)
- `{1:05}` left-pads the value with zeros to a minimum width of 5
- `{{` and `}}` are literal braces

For a single chunk stored under a fixed key, see [`ConstantChunkKeyEncoding`](@ref).

A chunk coordinate length the template cannot encode (see [`validate_template`](@ref))
raises an `ArgumentError`.

# Examples
```julia
e = TemplateChunkKeyEncoding("c/{*}", "/")
citostring(e, CartesianIndex(3, 2, 1))  # "c/0/1/2"
```
"""
struct TemplateChunkKeyEncoding <: AbstractChunkKeyEncoding
    format::String
    separator::Union{String,Nothing}
    tokens::Vector{Union{String,TemplateExpr}}
    function TemplateChunkKeyEncoding(format::AbstractString, separator::Union{AbstractString,Nothing}=nothing)
        tokens = _parse_template(format)
        nstar = count(t -> t isa TemplateExpr && t.index === nothing, tokens)
        nstar <= 1 || throw(ArgumentError("template chunk key encoding allows at most one {*}, got $nstar in \"$format\""))
        if nstar == 1 && separator === nothing
            throw(ArgumentError("template chunk key encoding with {*} requires a `separator`: \"$format\""))
        end
        new(String(format), separator === nothing ? nothing : String(separator), tokens)
    end
end

Base.:(==)(a::TemplateChunkKeyEncoding, b::TemplateChunkKeyEncoding) =
    a.format == b.format && a.separator == b.separator
Base.hash(a::TemplateChunkKeyEncoding, h::UInt) =
    hash(a.separator, hash(a.format, hash(:TemplateChunkKeyEncoding, h)))

const _TEMPLATE_EXPR_REGEX = r"^(\*|0|-?[1-9][0-9]*)(?::0([1-9][0-9]*))?$"

function _parse_template(fmt::AbstractString)
    tokens = Union{String,TemplateExpr}[]
    lit = IOBuffer()        # pending literal text (unescaped)
    skeleton = IOBuffer()   # literal text with 'x' standing in for each expression
    flush_literal() = position(lit) > 0 && push!(tokens, String(take!(lit)))
    i, n = firstindex(fmt), lastindex(fmt)
    while i <= n
        c = fmt[i]
        j = nextind(fmt, i)
        if c == '{'
            if j <= n && fmt[j] == '{'
                write(lit, '{'); write(skeleton, '{')
                i = nextind(fmt, j)
            else
                k = findnext('}', fmt, j)
                k === nothing && throw(ArgumentError("unclosed '{' in template \"$fmt\""))
                body = SubString(fmt, j, prevind(fmt, k))
                m = match(_TEMPLATE_EXPR_REGEX, body)
                m === nothing && throw(ArgumentError("invalid template expression {$body} in \"$fmt\""))
                flush_literal()
                idx = m.captures[1] == "*" ? nothing : parse(Int, m.captures[1])
                width = m.captures[2] === nothing ? 0 : parse(Int, m.captures[2])
                push!(tokens, TemplateExpr(idx, width))
                write(skeleton, 'x')
                i = nextind(fmt, k)
            end
        elseif c == '}'
            (j <= n && fmt[j] == '}') ||
                throw(ArgumentError("unmatched '}' in template \"$fmt\" (use '}}' for a literal brace)"))
            write(lit, '}'); write(skeleton, '}')
            i = nextind(fmt, j)
        else
            write(lit, c); write(skeleton, c)
            i = j
        end
    end
    flush_literal()
    any(t -> t isa TemplateExpr, tokens) ||
        throw(ArgumentError("template \"$fmt\" must contain at least one {} expression"))
    _check_chunk_key_literal(String(take!(skeleton)), "template \"$fmt\"")
    return tokens
end

# Lexical rules shared by the literal parts of template keys and by constant keys.
function _check_chunk_key_literal(s::AbstractString, what::AbstractString)
    startswith(s, '/') && throw(ArgumentError("$what must not begin with '/'"))
    for bad in ("..", "\\", "\0", "//", "zarr.json")
        occursin(bad, s) && throw(ArgumentError("$what must not contain $(repr(bad))"))
    end
    return nothing
end

# Validate the template against a coordinate of length `N`; returns a BitVector marking
# which coordinate positions are referenced by explicit (non-catch-all) expressions.
function _template_used(e::TemplateChunkKeyEncoding, N::Int)
    used = falses(N)
    nstar = 0
    for t in e.tokens
        t isa TemplateExpr || continue
        if t.index === nothing
            nstar += 1
            continue
        end
        p = t.index >= 0 ? t.index + 1 : N + t.index + 1
        1 <= p <= N || throw(ArgumentError(
            "index {$(t.index)} out of range for a chunk coordinate of length $N in template \"$(e.format)\""))
        used[p] && throw(ArgumentError(
            "multiple expressions resolve to coordinate position $(p-1) in template \"$(e.format)\" for length $N"))
        used[p] = true
    end
    if nstar == 0
        all(used) || throw(ArgumentError(
            "template \"$(e.format)\" does not cover all $N chunk coordinate values (add a {*}?)"))
    elseif all(used)
        throw(ArgumentError(
            "{*} in template \"$(e.format)\" would be populated by 0 values for a chunk coordinate of length $N"))
    end
    return used
end

"""
    validate_template(e::TemplateChunkKeyEncoding, ndims::Integer)

Throw an `ArgumentError` if the template cannot encode chunk coordinates of length `ndims`.
"""
function validate_template(e::TemplateChunkKeyEncoding, N::Integer)
    _template_used(e, Int(N))
    return nothing
end

"""
    validate_chunk_key_encoding(e::AbstractChunkKeyEncoding, ndims::Integer, nchunks::Integer)

Throw an `ArgumentError` if `e` cannot address an array with `ndims` dimensions and
`nchunks` chunks in total. Called when array metadata is parsed so that incongruent
metadata is rejected when the array is opened. The default method accepts everything.
"""
validate_chunk_key_encoding(::AbstractChunkKeyEncoding, ::Integer, ::Integer) = nothing
validate_chunk_key_encoding(e::TemplateChunkKeyEncoding, N::Integer, ::Integer) = validate_template(e, N)

_template_pad(v::Integer, w::Int) = lpad(string(v), w, '0')

function citostring(e::TemplateChunkKeyEncoding, i::CartesianIndex{N}) where N
    # Template indices refer to the Zarr (row-major) coordinate
    coord = reverse((i - oneunit(i)).I)
    used = _template_used(e, N)
    io = IOBuffer()
    for t in e.tokens
        if t isa String
            write(io, t)
        elseif t.index === nothing
            isfirst = true
            for k in 1:N
                used[k] && continue
                isfirst || write(io, e.separator)
                isfirst = false
                write(io, _template_pad(coord[k], t.width))
            end
        else
            p = t.index >= 0 ? t.index + 1 : N + t.index + 1
            write(io, _template_pad(coord[p], t.width))
        end
    end
    return String(take!(io))
end

function lower_chunk_key_encoding(e::TemplateChunkKeyEncoding)
    config = Dict{String,Any}("format" => e.format)
    e.separator === nothing || (config["separator"] = e.separator)
    Dict{String,Any}("name" => "template", "configuration" => config)
end

"""
    ConstantChunkKeyEncoding(key::AbstractString)

Chunk key encoding that stores the single chunk of an array under a fixed `key`,
independent of the chunk coordinate. It is meant for arrays consisting of exactly one
chunk or shard stored as one file, which may be a foreign file such as HDF5 or CZI.

The key is taken literally (no interpolation, braces are not special). It must not begin
or end with `/` and must not contain `..`, `\\`, `\\0`, `//` or `zarr.json`.

Only the chunk at the origin can be addressed; any other chunk raises an `ArgumentError`,
and [`validate_chunk_key_encoding`](@ref) rejects arrays with more than one chunk when
metadata is parsed. For arrays with several chunks use [`TemplateChunkKeyEncoding`](@ref).
"""
struct ConstantChunkKeyEncoding <: AbstractChunkKeyEncoding
    key::String
    function ConstantChunkKeyEncoding(key::AbstractString)
        isempty(key) && throw(ArgumentError("constant chunk key encoding requires a non-empty key"))
        _check_chunk_key_literal(key, "constant chunk key \"$key\"")
        endswith(key, '/') && throw(ArgumentError("constant chunk key \"$key\" must not end with '/'"))
        new(String(key))
    end
end

function validate_chunk_key_encoding(e::ConstantChunkKeyEncoding, ::Integer, nchunks::Integer)
    nchunks <= 1 || throw(ArgumentError(
        "constant chunk key \"$(e.key)\" can only address a single chunk, but the array has $nchunks chunks"))
    return nothing
end

function citostring(e::ConstantChunkKeyEncoding, i::CartesianIndex)
    all(isone, Tuple(i)) || throw(ArgumentError(
        "constant chunk key \"$(e.key)\" can only address the single chunk at the origin, not chunk $(reverse((i - oneunit(i)).I))"))
    return e.key
end

lower_chunk_key_encoding(e::ConstantChunkKeyEncoding) =
    Dict{String,Any}("name" => "constant", "configuration" => Dict{String,Any}("key" => e.key))

"""Serialize an `AbstractChunkKeyEncoding` to a JSON-compatible dict."""
function lower_chunk_key_encoding(e::ChunkKeyEncoding)
    Dict{String,Any}(
        "name" => e.prefix ? "default" : "v2",
        "configuration" => Dict{String,Any}("separator" => string(e.sep))
    )
end

function lower_chunk_key_encoding(e::SuffixChunkKeyEncoding)
    Dict{String,Any}(
        "name" => "suffix",
        "configuration" => Dict{String,Any}(
            "suffix" => e.suffix,
            "base_encoding" => lower_chunk_key_encoding(e.base_encoding)
        )
    )
end

"""Stores a registered chunk key encoding parser together with its expected return type."""
struct ChunkKeyEncodingEntry
    return_type::Type{<:AbstractChunkKeyEncoding}
    parser::Function
end

"""
Registry mapping chunk key encoding names to `ChunkKeyEncodingEntry` values
(return type + parser function). Use `register_chunk_key_encoding` to add new entries.
"""
const chunk_key_encoding_parsers = Dict{String, ChunkKeyEncodingEntry}()

"""
    register_chunk_key_encoding(parser::Function, name::String[, ::Type{T}])

Register a chunk key encoding parser under `name`. The parser must accept a
`Dict{String,Any}` configuration and return an `AbstractChunkKeyEncoding`.

The optional trailing `Type{T}` argument narrows the declared return type stored
in the registry (defaults to `AbstractChunkKeyEncoding`). Specifying it enables
a runtime assertion in `parse_chunk_key_encoding` and makes the registry
self-documenting.

Supports do-block syntax:

    register_chunk_key_encoding("myenc") do config
        MyEncoding(config["param"])
    end

    register_chunk_key_encoding("myenc", MyEncoding) do config
        MyEncoding(config["param"])
    end
"""
function register_chunk_key_encoding(parser::Function, name::String, ::Type{T}) where {T<:AbstractChunkKeyEncoding}
    chunk_key_encoding_parsers[name] = ChunkKeyEncodingEntry(T, parser)
end
register_chunk_key_encoding(parser::Function, name::String) =
    register_chunk_key_encoding(parser, name, AbstractChunkKeyEncoding)

"""
    parse_chunk_key_encoding(d::AbstractDict) -> AbstractChunkKeyEncoding

Parse a chunk key encoding dict (as found in `zarr.json`) into an
`AbstractChunkKeyEncoding` by looking up the registered parser for the encoding name.
"""
function parse_chunk_key_encoding(d::AbstractDict)::AbstractChunkKeyEncoding
    name = d["name"]
    config = get(d, "configuration", Dict{String,Any}())::Dict{String,Any}
    haskey(chunk_key_encoding_parsers, name) ||
        throw(ArgumentError("Unknown chunk_key_encoding of name, $name"))
    entry = chunk_key_encoding_parsers[name]
    return entry.parser(config)::entry.return_type
end

# Register built-in encodings
register_chunk_key_encoding("default", ChunkKeyEncoding) do config
    ChunkKeyEncoding(only(get(config, "separator", '/')), true)
end

register_chunk_key_encoding("v2", ChunkKeyEncoding) do config
    ChunkKeyEncoding(only(get(config, "separator", '.')), false)
end

register_chunk_key_encoding("suffix", SuffixChunkKeyEncoding) do config
    suffix_str = config["suffix"]
    base = parse_chunk_key_encoding(config["base_encoding"])
    SuffixChunkKeyEncoding(suffix_str, base)
end

register_chunk_key_encoding("template", TemplateChunkKeyEncoding) do config
    # The zarr-extensions schema names this field `format`; the README table says `template`.
    fmt = get(config, "format", get(config, "template", nothing))
    fmt === nothing && throw(ArgumentError("template chunk key encoding requires a \"format\" configuration field"))
    TemplateChunkKeyEncoding(fmt, get(config, "separator", nothing))
end

register_chunk_key_encoding("constant", ConstantChunkKeyEncoding) do config
    haskey(config, "key") || throw(ArgumentError("constant chunk key encoding requires a \"key\" configuration field"))
    ConstantChunkKeyEncoding(config["key"])
end

_concatpath(p,s) = isempty(p) ? s : rstrip(p,'/') * '/' * s
