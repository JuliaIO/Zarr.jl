using LMDB: LMDB

struct LMDBStore <: AbstractStore
    d::LMDB.LMDBDict{String,Vector{UInt8}}
end
function LMDBStore(p::String; create = false, readonly=false)
    if create
        ispath(p) && throw(ArgumentError("Path at $p already exists"))
        mkpath(p)
    end
    LMDBStore(LMDB.LMDBDict{String,Vector{UInt8}}(p, readonly=readonly))
end
Base.show(io::IO,d::LMDBStore) = print(io,"LMDB Database at $(d.d.env.path)")

Base.getindex(d::LMDBStore, i) = d.d[i]
Base.setindex!(d::LMDBStore, v, i) = setindex!(d.d,v,i)
Base.delete!(d::LMDBStore,i) = delete!(d.d,i)
storagesize(d::LMDBStore, p) = Int(LMDB.valuesize(d.d,prefix=p) - LMDB.valuesize(d.d,prefix=p*"/.zarray") - LMDB.valuesize(d.d,prefix=p*"/.zattrs"))
isinitialized(d::LMDBStore, p) = haskey(d.d,p)
function listwholefolder(d::LMDBStore, p)
    if !isempty(p) && !endswith(p,'/')
        p = string(p,'/')
    end
    LMDB.list_dirs(d.d, prefix = p, sep='/')
end
function subdirs(d::LMDBStore, p)
    rstrip.(filter(endswith('/'), listwholefolder(d,p)),'/')
end
function subkeys(d::LMDBStore, p)
    filter(!endswith('/'), listwholefolder(d,p))
end
function storefromstring(::Type{<:LMDBStore}, s, create)
    LMDBStore(s; create=create),""
end

push!(storageregexlist,r".lmdb/$"=>LMDBStore)
push!(storageregexlist,r".lmdb$"=>LMDBStore)
