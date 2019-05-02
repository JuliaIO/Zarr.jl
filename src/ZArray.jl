import JSON

getfillval(target::Type{T}, t::String) where {T <: Number} = parse(T, t)
getfillval(target::Type{T}, t::Union{T,Nothing}) where {T} = t

struct SenMissArray{T,N,V} <: AbstractArray{Union{T,Missing},N}
  x::Array{T,N}
end
SenMissArray(x::Array{T,N},v) where {T,N} = SenMissArray{T,N,convert(T,v)}(x)
Base.size(x::SenMissArray) = size(x.x)
senval(x::SenMissArray{<:Any,<:Any,V}) where V = V
function Base.getindex(x::SenMissArray,i::Int)
  v = x.x[i]
  isequal(v,senval(x)) ? missing : v
end
Base.setindex!(x::SenMissArray,v,i::Int) = x.x[i] = v
Base.setindex!(x::SenMissArray,::Missing,i::Int) = x.x[i] = senval(x)
Base.IndexStyle(::Type{<:SenMissArray})=Base.IndexLinear()

# Struct representing a Zarr Array in Julia, note that
# chunks(chunk size) and size are always in Julia column-major order
# Currently this is not an AbstractArray, because indexing single elements is
# would be really slow, although most AbstractArray interface functions are implemented
struct ZArray{T, N, C<:Compressor, S<:AbstractStore}
    metadata::Metadata{T, N, C}
    storage::S
    attrs::Dict
    writeable::Bool
end

Base.eltype(::ZArray{T}) where {T} = T
Base.ndims(::ZArray{<:Any,N}) where {N} = N
Base.size(z::ZArray) = z.metadata.shape
Base.size(z::ZArray,i) = z.metadata.shape[i]
Base.length(z::ZArray) = prod(z.metadata.shape)
Base.lastindex(z::ZArray,n) = size(z,n)
Base.lastindex(z::ZArray{<:Any,1}) = size(z,1)

function Base.show(io::IO,z::ZArray)
    print(io, "ZArray{", eltype(z) ,"} of size ",join(string.(size(z)), " x "))
end

zname(z::ZArray) = zname(z.storage)

"""
    storagesize(z::ZArray)

Returns the size of the compressed data stored in the ZArray `z` in bytes
"""
storagesize(z::ZArray) = storagesize(z.storage)

"""
    storageratio(z::ZArray)

Returns the ratio of the size of the uncompressed data in `z` and the size of the compressed data.
"""
storageratio(z::ZArray) = length(z)*sizeof(eltype(z))/storagesize(z)

zinfo(z::ZArray) = zinfo(stdout,z)
function zinfo(io::IO,z::ZArray)
  ninit = sum(chunkindices(z)) do i
    isinitialized(z.storage,i)
  end
  allinfos = [
    "Type" => "ZArray",
    "Data type" => eltype(z),
    "Shape" => size(z),
    "Chunk Shape" => z.metadata.chunks,
    "Order" => z.metadata.order,
    "Read-Only" => !z.writeable,
    "Compressor" => z.metadata.compressor,
    "Store type" => z.storage,
    "No. bytes"  => length(z)*sizeof(eltype(z)),
    "No. bytes stored" => storagesize(z),
    "Storage ratio" => storageratio(z),
    "Chunks initialized" => "$(ninit)/$(length(chunkindices(z)))"
  ]
  foreach(allinfos) do ii
    println(io,rpad(ii[1],20),": ",ii[2])
  end
end

# Construction of a ZArray given a folder on a regular drive
# A lot of the JSON parsing should be moved to a function, since
# this will be the same for other backends
function ZArray(s::T, mode="r") where T <: AbstractStore
  metadata = getmetadata(s)
  attrs = getattrs(s)
  writeable = mode == "w"
  ZArray{eltype(metadata), length(metadata.shape), typeof(metadata.compressor), T}(
    metadata, s, attrs, writeable)
end

"""
    convert_index(i,s)

Basic function to translate indices given by the user
to unit ranges.
"""
convert_index(i::Integer, s::Int) = i:i
convert_index(i::AbstractUnitRange, s::Int) = i
convert_index(::Colon, s::Int) = Base.OneTo(s)

# Helper function for reshaping the result in the end
convert_index2(::Colon, s) = Base.OneTo(s)
convert_index2(i, s) = i

"""
    trans_ind(r, bs)

For a given index and blocksize determines which chunks of the Zarray will have to
be accessed.
"""
trans_ind(r::AbstractUnitRange, bs) = ((first(r) - 1) ÷ bs):((last(r) - 1) ÷ bs)
trans_ind(r::Integer, bs) = (r - 1) ÷ bs

"""
Most important helper function. Returns two tuples of ranges.
For a given chunks bI it determines the indices
to read inside the chunk `i1:i2` as well as the indices to write to in the return
array `io1:io2`.
"""
function inds_in_block(r::CartesianIndices{N}, # Outer Array indices to read
        bI::CartesianIndex{N}, # Index of the current block to read
        blockAll::CartesianIndices{N}, # All blocks to read
        c::NTuple{N, Int}, # Chunks
        enumI::CartesianIndices{N}, # Idices of all block to read
        offsfirst::NTuple{N, Int} # Offset of the first block to read
        ) where {N}
    sI = size(enumI)
    map(r.indices, bI.I, blockAll.indices, c, enumI.indices, sI, offsfirst) do iouter,
            iblock, ablock, chunk, enu, senu, o0

        if iblock == first(ablock)
            i1 = mod1(first(iouter), chunk)
            io1 = 1
        else
            i1 = 1
            io1 = (iblock - first(ablock)) * chunk - o0 + 2
        end
        if iblock == last(ablock)
            i2 = mod1(last(iouter), chunk)
            io2 = length(iouter)
        else
            i2 = chunk
            io2 = (iblock - first(ablock) + 1) * chunk - o0 + 1
        end
        i1:i2, io1:io2
    end
end

function getchunkarray(z::ZArray{>:Missing})
    # temporary workaround to use strings as data values
    inner = zeros(Base.nonmissingtype(eltype(z)), z.metadata.chunks)
    a = SenMissArray(inner,z.metadata.fill_value)
end



getchunkarray(z::ZArray) = zeros(eltype(z), z.metadata.chunks)

maybeinner(a::Array) = a
maybeinner(a::SenMissArray) = a.x
# Function to read or write from a zarr array. Could be refactored
# using type system to get rid of the `if readmode` statements.
function readblock!(aout, z::ZArray{<:Any, N}, r::CartesianIndices{N}; readmode=true) where {N}
    if !readmode && !z.writeable
        error("Trying to write to read-only ZArray")
    end
    # Determines which chunks are affected
    blockr = CartesianIndices(map(trans_ind, r.indices, z.metadata.chunks))
    enumI = CartesianIndices(blockr)
    # Get the offset of the first index in each dimension
    offsfirst = map((a, bs) -> mod(first(a) - 1, bs) + 1, r.indices, z.metadata.chunks)
    # Allocate array of the size of a chunks where uncompressed data can be held
    a = getchunkarray(z)
    # Get linear indices from user array. This is a workaround to make something
    # like z[:] = 1:10 work, because a unit range can not be accessed through
    # CartesianIndices
    if !readmode
          linoutinds = LinearIndices(r)
    end
    # Now loop through the chunks
    for bI in blockr

        # Get indices to extract and to write to for the current chunk
        ii = inds_in_block(r, bI, blockr, z.metadata.chunks, enumI, offsfirst)
        # Extract them as CartesianIndices objects
        i_in_a = CartesianIndices(map(i -> i[1], ii))
        i_in_out = CartesianIndices(map(i -> i[2], ii))
        # Uncompress a chunk
        if readmode || (isinitialized(z.storage, bI + one(bI)) && (size(i_in_a) != size(a)))
          readchunk!(maybeinner(a), z, bI + one(bI))
        end

        if readmode
            # Read data
            copyto!(aout,i_in_out,a,i_in_a)
        else
            # Write data, here one could dispatch on the IndexStyle
            # Of the user-provided array, and then decide on an
            # Indexing style
            a[i_in_a] .= extractreadinds(aout, linoutinds, i_in_out)
            writechunk!(maybeinner(a), z, bI + one(bI))
        end
    end
    aout
end

replace_missings!(a,v)=nothing
replace_missings!(::AbstractArray{>:Missing},::Nothing)=nothing
replace_missings!(a::AbstractArray{>:Missing},v)=replace!(a,v=>missing)

# Some helper functions to determine the shape of the output array
gets(x::Tuple) = gets(x...)
gets(x::AbstractRange, r...) = (length(x), gets(r...)...)
gets(x::Integer, r...) = gets(r...)
gets() = ()

# Short wrapper around readblock! to have getindex-style behavior
function Base.getindex(z::ZArray{T}, i::Int...) where {T}
    ii = CartesianIndices(map(convert_index, i, size(z)))
    # temporary workaround to strings as a data values
    aout = zeros(T, size(ii))
    readblock!(aout, z, ii)
    aout[1]
end

function Base.getindex(z::ZArray{T}, i...) where {T}
    ii = CartesianIndices(map(convert_index, i, size(z)))
    aout = Array{T}(undef, size(ii))
    readblock!(aout, z, ii)
    ii2 = map(convert_index2, i, size(z))
    reshape(aout, gets(ii2))
end

function Base.getindex(z::ZArray{T,1}, ::Colon) where {T}
    ii = CartesianIndices(size(z))
    aout = zeros(T, size(ii))
    readblock!(aout, z, ii)
    reshape(aout, length(aout))
end

# Method for getting a UnitRange of indices is missing

function Base.setindex!(z::ZArray, v, i...)
    ii = CartesianIndices(map(convert_index, i, size(z)))
    readblock!(v, z, ii, readmode=false)
end

function Base.setindex!(z::ZArray,v,::Colon)
    ii = CartesianIndices(size(z))
    readblock!(v, z, ii, readmode=false)
end

"""
    readchunk!(a::DenseArray{T},z::ZArray{T,N},i::CartesianIndex{N})

Read the chunk specified by `i` from the Zarray `z` and write its content to `a`
"""
function readchunk!(a::DenseArray,z::ZArray{<:Any,N},i::CartesianIndex{N}) where N
    length(a) == prod(z.metadata.chunks) || throw(DimensionMismatch("Array size does not equal chunk size"))
    curchunk = z.storage[i]
    if curchunk === nothing
        fill!(a, z.metadata.fill_value)
    else
        zuncompress(a, curchunk, z.metadata.compressor)
    end
    a
end

"""
    writechunk!(a::DenseArray{T},z::ZArray{T,N},i::CartesianIndex{N}) where {T,N}

Write the data from the array `a` to the chunk `i` in the ZArray `z`
"""
function writechunk!(a::DenseArray, z::ZArray{<:Any,N}, i::CartesianIndex{N}) where N
    z.writeable || error("ZArray not in write mode")
    length(a) == prod(z.metadata.chunks) || throw(DimensionMismatch("Array size does not equal chunk size"))
    dtemp = UInt8[]
    zcompress(a,dtemp,z.metadata.compressor)
    z.storage[i]=dtemp
    a
end

function extractreadinds(a,linoutinds,i_in_out)
    i_in_out2 = linoutinds[i_in_out]
    a[i_in_out2]
end

extractreadinds(a::Number, linoutinds, i_in_out) = a

"""
    zcreate(T, dims...;kwargs)

Creates a new empty zarr aray with element type `T` and array dimensions `dims`. The following keyword arguments are accepted:

* `path=""` directory name to store a persistent array. If left empty, an in-memory array will be created
* `name=""` name of the zarr array, defaults to the directory name
* `storagetype` determines the storage to use, current options are `DirectoryStore` or `DictStore`
* `chunks=dims` size of the individual array chunks, must be a tuple of length `length(dims)`
* `fill_value=nothing` value to represent missing values
* `compressor=BloscCompressor()` compressor type and properties
* `attrs=Dict()` a dict containing key-value pairs with metadata attributes associated to the array
* `writeable=true` determines if the array is opened in read-only or write mode
"""
function zcreate(::Type{T}, dims...;
        name="",
        path=nothing,
        kwargs...
        ) where T
  if path===nothing
    store = DictStore("")
  else
    store = DirectoryStore(joinpath(path,name))
  end
  zcreate(T, store, dims...; kwargs...)
end

function zcreate(::Type{T},storage::AbstractStore,
        dims...;
        chunks=dims,
        fill_value=nothing,
        compressor=BloscCompressor(),
        attrs=Dict(),
        writeable=true,
        ) where T

    length(dims) == length(chunks) || throw(DimensionMismatch("Dims must have the same length as chunks"))
    N = length(dims)
    C = typeof(compressor)
    metadata = Metadata{T, N, C}(
        2,
        dims,
        chunks,
        typestr(T),
        compressor,
        fill_value,
        'C',
        nothing
    )

    isempty(storage) || error("$storage is not empty")

    writemetadata(storage, metadata)

    writeattrs(storage, attrs)

    z = ZArray{T, N, typeof(compressor), typeof(storage)}(
        metadata, storage, attrs, writeable)
end

"""
    chunkindices(z::ZArray)

Returns the Cartesian Indices of the chunks of a given ZArray
"""
chunkindices(z::ZArray) = CartesianIndices(map((s, c) -> 1:ceil(Int, s/c), z.metadata.shape, z.metadata.chunks))

"""
    zzeros(T, dims..., )

Creates a zarr array and initializes all values with zero.
"""
function zzeros(T,dims...;kwargs...)
  z = zcreate(T,dims...;kwargs...)
  as = zeros(T, z.metadata.chunks...)
  for i in chunkindices(z)
      writechunk!(as, z, i)
  end
  z
end
