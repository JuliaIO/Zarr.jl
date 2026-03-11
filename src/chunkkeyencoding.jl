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

"""
Registry mapping chunk key encoding names to parser functions.

Each parser function takes a configuration `Dict{String,Any}` and returns an
`AbstractChunkKeyEncoding`. Use `register_chunk_key_encoding` to add new entries.
"""
const chunk_key_encoding_parsers = Dict{String, Function}()

"""
    register_chunk_key_encoding(parser::Function, name::String)

Register a chunk key encoding parser under `name`. The parser must accept a
`Dict{String,Any}` configuration and return an `AbstractChunkKeyEncoding`.

Supports do-block syntax:

    register_chunk_key_encoding("myenc") do config
        MyEncoding(config["param"])
    end
"""
function register_chunk_key_encoding(parser::Function, name::String)
    chunk_key_encoding_parsers[name] = parser
end

"""
    parse_chunk_key_encoding(d::AbstractDict) -> AbstractChunkKeyEncoding

Parse a chunk key encoding dict (as found in `zarr.json`) into an
`AbstractChunkKeyEncoding` by looking up the registered parser for the encoding name.
"""
function parse_chunk_key_encoding(d::AbstractDict)::AbstractChunkKeyEncoding
    name = d["name"]
    config = get(d, "configuration", Dict{String,Any}())::Dict{String,Any}
    if haskey(chunk_key_encoding_parsers, name)
        return chunk_key_encoding_parsers[name](config)
    else
        throw(ArgumentError("Unknown chunk_key_encoding of name, $name"))
    end
end

# Register built-in encodings
register_chunk_key_encoding("default") do config
    ChunkKeyEncoding(only(get(config, "separator", '/')), true)
end

register_chunk_key_encoding("v2") do config
    ChunkKeyEncoding(only(get(config, "separator", '.')), false)
end

register_chunk_key_encoding("suffix") do config
    suffix_str = config["suffix"]
    base = parse_chunk_key_encoding(config["base_encoding"])
    SuffixChunkKeyEncoding(suffix_str, base)
end

_concatpath(p,s) = isempty(p) ? s : rstrip(p,'/') * '/' * s
