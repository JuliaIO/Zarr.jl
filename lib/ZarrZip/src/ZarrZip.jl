"""
    ZarrZip

Zip archive support for Zarr.jl: the read-only [`ZipStore`](@ref) and
[`writezip`](@ref), which serialises any `AbstractStore` into a zip archive.

This is a subpackage of Zarr.jl; its public API is re-exported by `Zarr`, so
`Zarr.ZipStore` and `Zarr.writezip` keep working exactly as before.
"""
module ZarrZip

import ZipArchives

# Only the names that are used unqualified live here. Methods that *extend* a
# ZarrCore generic are always written as `ZarrCore.f(...)` below: writing a bare
# `f(...)` definition would silently create a new `ZarrZip.f` that shadows the
# generic instead of adding a method to it, and `zopen` would then never see it.
import ZarrCore
using ZarrCore: AbstractStore, ZArray, ZGroup, subdirs, subkeys

"""
    ZipStore

A read only store that wraps an `AbstractVector{UInt8}` that contains a zip file.
"""
struct ZipStore{T <: AbstractVector{UInt8}} <: AbstractStore
    r::ZipArchives.ZipBufferReader{T}
end


ZipStore(data::AbstractVector{UInt8}) = ZipStore(ZipArchives.ZipBufferReader(data))

Base.show(io::IO,::ZipStore) = print(io,"Read Only Zip Storage")

function Base.getindex(d::ZipStore, k::AbstractString)::Union{Nothing, Vector{UInt8}}
    i = ZipArchives.zip_findlast_entry(d.r, k)
    if isnothing(i)
        nothing
    else
        ZipArchives.zip_readentry(d.r, i)
    end
end

_make_prefix(p)::String =(isempty(p) || endswith(p,'/')) ? p : p*'/'

function ZarrCore.storagesize(d::ZipStore, p)::Int64
    prefix::String = _make_prefix(p)
    s::Int128 = Int128(0)
    for i in 1:ZipArchives.zip_nentries(d.r)
        name = ZipArchives.zip_name(d.r, i)
        if startswith(name, prefix)
            filename = last(split(name, '/'))
            if !in(filename,(".zattrs",".zarray",".zgroup"))
                s += ZipArchives.zip_uncompressed_size(d.r, i)
            end
        end
    end
    s
end

function ZarrCore.subdirs(d::ZipStore, p)::Vector{String}
    prefix::String = _make_prefix(p)
    o = Set{String}()
    for i in 1:ZipArchives.zip_nentries(d.r)
        name = ZipArchives.zip_name(d.r, i)
        if startswith(name, prefix) && !endswith(name, '/')
            chopped_name = SubString(name, 1+ncodeunits(prefix))
            if '/' ∈ chopped_name
                push!(o, first(split(chopped_name, '/')))
            end
        end
    end
    collect(o)
end
function ZarrCore.subkeys(d::ZipStore, p)::Vector{String}
    prefix::String = _make_prefix(p)
    o = Set{String}()
    for i in 1:ZipArchives.zip_nentries(d.r)
        name = ZipArchives.zip_name(d.r, i)
        if startswith(name, prefix) && !endswith(name, '/')
            chopped_name = SubString(name, 1+ncodeunits(prefix))
            if '/' ∉ chopped_name
                push!(o, chopped_name)
            end
        end
    end
    collect(o)
end

# Zip archives are generally append only
# so it doesn't quite work to make ZipStore writable.
# The idea is if you want a zipfile, you should first use one of the
# regular mutable stores, then save it to a zip archive.
"""
    writezip(io::IO, s::AbstractStore, p)
    writezip(io::IO, s::Union{ZArray,ZGroup})

Write an AbstractStore to an IO as a zip archive.
"""
function writezip(io::IO, s::AbstractStore, p=""; kwargs...)
    ZipArchives.ZipWriter(io; kwargs...) do w
        _writezip(w, s, String(p))
    end
end
writezip(io::IO, s::Union{ZArray,ZGroup}; kwargs...) = writezip(io, s.storage, s.path; kwargs...)
function _writezip(w::ZipArchives.ZipWriter, s::AbstractStore, p::String)
    for subkey in subkeys(s, p)
        fullname = _make_prefix(p)*subkey
        data = getindex(s, fullname)
        if !isnothing(data)
            ZipArchives.zip_writefile(w, fullname, data)
        end
    end
    for subdir in subdirs(s, p)
        _writezip(w, s, _make_prefix(p)*subdir)
    end
end

# `ZipStore` has no `storageregexlist` entry (a zip archive is not addressable
# by a URL scheme), so there is nothing to register at load time and this module
# deliberately has no `__init__`.
@static if VERSION >= v"1.11"
    include("public_names_zip.jl")
end

end # module
