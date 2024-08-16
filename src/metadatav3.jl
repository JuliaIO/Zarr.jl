import JSON3: JSON3, StructTypes
import StringViews: StringView
struct NodeInfo
    zarr_format::Int
    node_type::String
end
NodeInfo(s::AbstractString) = JSON3.read(s, NodeInfo)
NodeInfo(s::AbstractVector{UInt8}) = NodeInfo(StringView(s))

struct NameAndConfig
    name::Symbol
    configuration::Dict{Symbol,Any}
end
abstract type ZarrSetting end
StructTypes.StructType(::Type{<:ZarrSetting}) = StructTypes.CustomStruct()
StructTypes.lowertype(::Type{<:ZarrSetting}) = NameAndConfig
StructTypes.StructType(::Type{NameAndConfig}) = StructTypes.Struct()
function StructTypes.lower(x::T) where {T<:ZarrSetting}
    NameAndConfig(name(T), Dict{Symbol,Any}(k => getproperty(x, k) for k in fieldnames(T)))
end
function StructTypes.construct(::Type{T}, x::NameAndConfig) where {T<:ZarrSetting}
    Tconcrete = StructTypes.subtypes(T)[x.name]
    Tconcrete(map(n -> get(x.configuration, n, nothing), fieldnames(Tconcrete))...)
end


abstract type ChunkGrid <: ZarrSetting end
struct RegularChunkGrid <: ChunkGrid
    chunk_shape::Vector{Int}
end
name(::Type{RegularChunkGrid}) = :regular
StructTypes.subtypes(::Type{ChunkGrid}) = (regular=RegularChunkGrid,)

abstract type ChunkKeyEncoding <: ZarrSetting end
struct DefaultChunkKeyEncoding <: ChunkKeyEncoding
    separator::Char
end
DefaultChunkKeyEncoding(::Nothing) = DefaultChunkKeyEncoding('/')
DefaultChunkKeyEncoding(s::String) = length(s) == 1 ? DefaultChunkKeyEncoding(only(s)) : error("Chunk Key separator must be a single Character")
name(::Type{DefaultChunkKeyEncoding}) = :default
StructTypes.subtypes(::Type{ChunkKeyEncoding}) = (default=DefaultChunkKeyEncoding,)


abstract type Codec <: ZarrSetting end
StructTypes.subtypes(::Type{Codec}) = (bytes=BytesCodec, blosc=BloscCodec)
abstract type BytesToBytes <: Codec end
abstract type ArrayToBytes <: Codec end
struct BytesCodec <: ArrayToBytes
    endian::String
end
name(::Type{BytesCodec}) = :bytes

struct BloscCodec <: BytesToBytes
    cname::String
    clevel::Int
    shuffle::String
    typesize::Int
    blocksize::Int
end
name(::Type{BloscCodec}) = :blosc

function metadata_and_attrs(v3json, ::V3)
    str = String(Zarr.maybecopy(v3json))
    allinfo = JSON3.read(str, V3Metadata)
    allinfo, allinfo.attributes
end

function metadata_and_attrs(s, path, fill_as_missing, version::V3; metadata_str=nothing)
    if metadata_str === nothing
        metadata_and_attrs(s[path, "zarr.json"], version)
    else
        metadata_and_attrs(metadata_str, version)
    end
end

struct ArrayInfoUntyped
    zarr_format::Int
    node_type::String
    shape::Vector{Int}
    data_type::String
    chunk_grid::Zarr.ChunkGrid
    chunk_key_encoding::Zarr.ChunkKeyEncoding
    fill_value::Any
    codecs::Vector{Zarr.Codec}
    attributes::Union{Nothing,Dict{String,Any}}
    storage_transformers::Union{Nothing,Vector{Zarr.NameAndConfig}}
    dimension_names::Union{Nothing,Vector{String}}
end


struct V3Metadata{T,N,CG,CK,CO,ST} <: AbstractMetadata{T,N}
    shape::Base.RefValue{NTuple{N,Int}}
    data_type::Type{T}
    chunk_grid::CG
    chunk_key_encoding::CK
    fill_value::T
    codecs::CO
    attributes::Dict{String,Any}
    storage_transformers::ST
    dimension_names::Union{Nothing,NTuple{N,String}}
end
function V3Metadata(i::ArrayInfoUntyped)
    T = zarrv3strtodatatype(i.data_type)
    i.zarr_format == 3 || error("Only v3 format is supported")
    i.node_type == "array" || error("Can only parse array metadata")
    V3Metadata(
        Ref((i.shape...,)),
        T,
        i.chunk_grid,
        i.chunk_key_encoding,
        T(i.fill_value),
        (i.codecs...,),
        i.attributes === nothing ? Dict{String,Any} : i.attributes,
        i.storage_transformers === nothing ? nothing : (i.storage_transformers...,),
        i.dimension_names === nothing ? nothing : (i.dimension_names...,)
    )
end
StructTypes.StructType(::Type{<:V3Metadata}) = StructTypes.CustomStruct()
StructTypes.lowertype(::Type{<:V3Metadata}) = ArrayInfoUntyped
function StructTypes.lower(x::V3Metadata)
    ArrayInfoUntyped(3, "array", collect(x.shape[]), zarrv3datatypetostr(x.data_type), x.chunk_grid, x.chunk_key_encoding,
        x.fill_value, collect(Zarr.Codec, x.codecs), isempty(x.attributes) ? nothing : x.attributes,
        isnothing(x.storage_transformers) || isempty(x.storage_transformers) ? nothing : x.storage_transformers, isnothing(x.dimension_names) ? nothing : collect(x.dimension_names))
end


const json_datatypes = (
    "bool" => Bool,
    "int8" => Int8, "int16" => Int16, "int32" => Int32, "int64" => Int64,
    "uint8" => UInt8, "uint16" => UInt16, "uint32" => UInt32, "uint64" => UInt64,
    "float16" => Float16, "float32" => Float32, "float64" => Float64,
    "complex64" => ComplexF32, "complex128" => ComplexF64,
)
function zarrv3strtodatatype(str)
    i = findfirst(i -> first(i) == str, json_datatypes)
    last(json_datatypes[i])
end
function zarrv3datatypetostr(T)
    i = findfirst(i -> last(i) <: T, json_datatypes)
    first(json_datatypes[i])
end

getattrs(::V3, _, _, metadata_str) = JSON3.read(metadata_str, @NamedTuple{attributes::Dict{String,Any}}).attributes
getattrs(::V2, s, p, _) = getattrs(s, p)