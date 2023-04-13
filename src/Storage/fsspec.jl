using PythonCall

"""
    FSStore
Load data that can be accessed through 
any filesystem supported by the fsspec python package.
"""
struct FSStore <: AbstractStore
    url::String
    mapper::Py
end

function FSStore(url::String; storage_options...)
    fsspec = pyimport("fsspec")
    fs, = fsspec.core.url_to_fs(url; storage_options...)
    mapper = fs.get_mapper(url)
    return FSStore(url, mapper)
end

function Base.getindex(s::FSStore, k::String)
   return pyconvert(Vector, s.mapper[k])
end

function read_items!(s::FSStore, c::AbstractChannel, p, i)
    cinds = [citostring(ii) for ii in i]
    ckeys = ["$p/$cind" for cind in cinds]
    cdatas = s.mapper.getitems(ckeys, on_error="omit")
    for ii in i
        put!(c,(ii => pyconvert(Vector, cdatas[ckeys[ii]])))
    end
end

function listdir(s::FSStore, p; nometa=true) 
    try
        listing = pyconvert(Vector, s.mapper.fs.listdir(p, detail=false))
        if nometa
          listing = [za for za in listing if !startswith(za, ".")]
        end
        return listing
    catch e
        return String[]
    end
end

struct PyConcurrentRead end

subdirs(s::FSStore, p) = listdir(s, p)
is_zarray(s::FSStore, p) = "$p/.zarray" in listdir(s, p, nometa=false)
is_zgroup(s::FSStore, p) = ".zgroup" in listdir(s, p, nometa=false)
store_read_strategy(::FSStore) = PyConcurrentRead()
channelsize(::PyConcurrentRead) = concurrent_io_tasks[] 
read_items!(s::FSStore, c::AbstractChannel, ::PyConcurrentRead, p, i) = read_items!(s, c, p, i)
read_items!(s::ConsolidatedStore, c::AbstractChannel, ::PyConcurrentRead, p, i) = read_items!(s.parent, c, p, i)

