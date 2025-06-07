# Stores data in a simple dict in memory
abstract type AbstractDictStore <: AbstractStore end

struct DictStore <: AbstractDictStore
    a::Dict{String,Vector{UInt8}}
end
DictStore() = DictStore(Dict{String,Vector{UInt8}}())

Base.show(io::IO, d::AbstractDictStore) = print(io, "Dictionary Storage")
function _pdict(d::AbstractDictStore, p)
    p = (isempty(p) || endswith(p, '/')) ? p : p * '/'
    filter(((k, v),) -> startswith(k, p), d.a)
end
function _pkeys(d::AbstractDictStore, p)
    p = (isempty(p) || endswith(p, '/')) ? p : p * '/'
    filter((k) -> startswith(k, p), keys(d.a))
end
function storagesize(d::AbstractDictStore, p)
    sum(i -> if last(split(i[1], '/')) ∉ (".zattrs", ".zarray")
        sizeof(i[2])
    else
        zero(sizeof(i[2]))
    end, _pdict(d, p))
end

function Base.getindex(d::AbstractDictStore, i::AbstractString)
    get(d.a, i, nothing)
end
function Base.setindex!(d::AbstractDictStore, v, i::AbstractString)
    d.a[i] = v
end
Base.delete!(d::AbstractDictStore, i::AbstractString) = delete!(d.a, i)

function subdirs(d::AbstractDictStore, p)
    d2 = _pkeys(d, p)
    _searchsubdict(d2, p, (sp, lp) -> length(sp) > lp + 1)
end

function subkeys(d::AbstractDictStore, p)
    d2 = _pkeys(d, p)
    _searchsubdict(d2, p, (sp, lp) -> length(sp) == lp + 1)
end

function _searchsubdict(d2, p, condition)
    o = Set{String}()
    pspl = split(rstrip(p, '/'), '/')
    lp = if length(pspl) == 1 && isempty(pspl[1])
        0
    else
        length(pspl)
    end
    for k in d2
        sp = split(k, '/')
        if condition(sp, lp)
            push!(o, sp[lp + 1])
        end
    end
    collect(o)
end

#getsub(d::AbstractDictStore, p, n) = _substore(d,p).subdirs[n]

#path(d::AbstractDictStore) = ""
