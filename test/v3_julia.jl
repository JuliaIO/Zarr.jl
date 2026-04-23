# Julia script to generate Zarr v3 fixtures using pure Julia
# Mirrors the examples from v3_python.jl

using Zarr
using JSON

# Paths
path_v3 = joinpath(@__DIR__, "v3_julia", "data.zarr")

# Remove existing
if isdir(path_v3)
    rm(path_v3, recursive=true)
end

# Create store and root group for v3
store = Zarr.DirectoryStore(path_v3)
g = zgroup(store, "", Zarr.ZarrFormat(3))

# Helper: create array and set data
function create_and_fill(store, name, data; 
    dtype=nothing, 
    shape=nothing, 
    chunks=nothing,
    compressor=Zarr.BloscCompressor(),
    fill_value=nothing,
    zarr_format=3,
    dimension_separator='/')

    # Create the array
    z = zcreate(eltype(data), store, shape...;
        path=name,
        chunks=chunks,
        compressor=compressor,
        fill_value=fill_value,
        zarr_format=zarr_format,
        dimension_separator=dimension_separator)
    # Fill the array with the data
    z[:] = data
    return z
end

# 1d.contiguous.gzip.i2
create_and_fill(store, "1d.contiguous.gzip.i2", Int16[1,2,3,4];
    shape=(4,),
    chunks=(4,),
    compressor=Zarr.ZlibCompressor(),
)

# 1d.contiguous.blosc.i2
create_and_fill(store, "1d.contiguous.blosc.i2", Int16[1,2,3,4];
    shape=(4,),
    chunks=(4,),
    compressor=Zarr.BloscCompressor(shuffle=0),  # noshuffle
)

# 1d.contiguous.raw.i2
create_and_fill(store, "1d.contiguous.raw.i2", Int16[1,2,3,4];
    shape=(4,),
    chunks=(4,),
    compressor=Zarr.NoCompressor(),
)

# 1d.contiguous.i4
create_and_fill(store, "1d.contiguous.i4", Int32[1,2,3,4];
    shape=(4,),
    chunks=(4,),
    compressor=Zarr.BloscCompressor(shuffle=0),  # noshuffle
)

# 1d.contiguous.u1
create_and_fill(store, "1d.contiguous.u1", UInt8[255,0,255,0];
    shape=(4,),
    chunks=(4,),
    compressor=Zarr.BloscCompressor(shuffle=0),  # noshuffle
)

# 1d.contiguous.f2.le
create_and_fill(store, "1d.contiguous.f2.le", Float16[-1000.5, 0.0, 1000.5, 0.0];
    shape=(4,),
    chunks=(4,),
    compressor=Zarr.BloscCompressor(shuffle=0),  # noshuffle
)

# 1d.contiguous.f4.le
create_and_fill(store, "1d.contiguous.f4.le", Float32[-1000.5, 0.0, 1000.5, 0.0];
    shape=(4,),
    chunks=(4,),
    compressor=Zarr.BloscCompressor(shuffle=0),  # noshuffle
)

# 1d.contiguous.f4.be
# Note: Big endian is not directly supported in Julia, but we can create the array
# The actual endianness is handled by the bytes codec in v3
create_and_fill(store, "1d.contiguous.f4.be", Float32[-1000.5, 0.0, 1000.5, 0.0];
    shape=(4,),
    chunks=(4,),
    compressor=Zarr.BloscCompressor(shuffle=0),  # noshuffle
)

# 1d.contiguous.f8
create_and_fill(store, "1d.contiguous.f8", Float64[1.5,2.5,3.5,4.5];
    shape=(4,),
    chunks=(4,),
    compressor=Zarr.BloscCompressor(shuffle=0),  # noshuffle
)

# 1d.contiguous.b1
create_and_fill(store, "1d.contiguous.b1", Bool[true,false,true,false];
    shape=(4,),
    chunks=(4,),
    compressor=Zarr.BloscCompressor(shuffle=0),  # noshuffle
)

# 1d.chunked.i2
z = create_and_fill(store, "1d.chunked.i2", Int16[1,2,3,4];
    shape=(4,),
    chunks=(2,),
    compressor=Zarr.BloscCompressor(shuffle=0),  # noshuffle
)

# Adjust zarr.json to set dimension_names = null
meta_path = joinpath(path_v3, "1d.chunked.i2", "zarr.json")
meta = JSON.parsefile(meta_path; dicttype = Dict{String,Any})
meta["dimension_names"] = nothing
open(meta_path, "w") do io
    JSON.print(io, meta)
end

# 1d.chunked.ragged.i2
create_and_fill(store, "1d.chunked.ragged.i2", Int16[1,2,3,4,5];
    shape=(5,),
    chunks=(2,),
    compressor=Zarr.BloscCompressor(shuffle=0),  # noshuffle
)

# 2d.contiguous.i2
create_and_fill(store, "2d.contiguous.i2", Int16[1 2; 3 4];
    shape=(2,2),
    chunks=(2,2),
    compressor=Zarr.BloscCompressor(shuffle=0),  # noshuffle
)

# 2d.chunked.i2
create_and_fill(store, "2d.chunked.i2", Int16[1 2; 3 4];
    shape=(2,2),
    chunks=(1,1),
    compressor=Zarr.BloscCompressor(shuffle=0),  # noshuffle
)

# 2d.chunked.ragged.i2
create_and_fill(store, "2d.chunked.ragged.i2", Int16[1 2 3; 4 5 6; 7 8 9];
    shape=(3,3),
    chunks=(2,2),
    compressor=Zarr.BloscCompressor(shuffle=0),  # noshuffle
)

# 3d.contiguous.i2
create_and_fill(store, "3d.contiguous.i2", reshape(Int16.(0:26), 3, 3, 3);
    shape=(3,3,3),
    chunks=(3,3,3),
    compressor=Zarr.BloscCompressor(shuffle=0),  # noshuffle
)

# 3d.chunked.i2
create_and_fill(store, "3d.chunked.i2", reshape(Int16.(0:26), 3, 3, 3);
    shape=(3,3,3),
    chunks=(1,1,1),
    compressor=Zarr.BloscCompressor(shuffle=0),  # noshuffle
)

# 3d.chunked.mixed.i2.C
create_and_fill(store, "3d.chunked.mixed.i2.C", reshape(Int16.(0:26), 3, 3, 3);
    shape=(3,3,3),
    chunks=(3,3,1),
    compressor=Zarr.BloscCompressor(shuffle=0),  # noshuffle
)

# 3d.chunked.mixed.i2.F
# Note: Column-major order (F) is simulated with transpose filter in Python
# In Julia, we create with C order as that's what's currently supported
create_and_fill(store, "3d.chunked.mixed.i2.F", reshape(Int16.(0:26), 3, 3, 3);
    shape=(3,3,3),
    chunks=(3,3,1),
    compressor=Zarr.BloscCompressor(shuffle=0),  # noshuffle
)

##### Sharded/compressed examples

# Helper: create a ZArray with ShardingCodec (gzip inner) and write data.
# outer_chunk_shape: shard shape in Julia column-major order
# inner_chunk_shape: inner chunk shape in Julia column-major order
# index_location: :end (default, most common) or :start
# index_crc32c: whether to include the CRC32c integrity codec in the shard index
function create_sharded(store, name, data, outer_chunk_shape, inner_chunk_shape;
        index_location::Symbol=:end, index_crc32c::Bool=true)
    T = eltype(data)
    N = ndims(data)
    inner_pipeline = Zarr.V3Pipeline(
        (),
        Zarr.Codecs.V3Codecs.BytesCodec(:little),
        (Zarr.Codecs.V3Codecs.GzipV3Codec(1),),
    )
    index_bytes_bytes = index_crc32c ? (Zarr.Codecs.V3Codecs.CRC32cV3Codec(),) : ()
    index_pipeline = Zarr.V3Pipeline(
        (),
        Zarr.Codecs.V3Codecs.BytesCodec(:little),
        index_bytes_bytes,
    )
    sharding = Zarr.Codecs.V3Codecs.ShardingCodec(inner_chunk_shape, inner_pipeline, index_pipeline, index_location)
    pipeline = Zarr.V3Pipeline((), sharding, ())
    md = Zarr.MetadataV3{T, N, typeof(pipeline)}(
        3, "array", size(data), outer_chunk_shape, Zarr.typestr3(T), pipeline, zero(T),
        Zarr.ChunkKeyEncoding('/', true),
    )
    z = Zarr.ZArray(md, store, name, Dict(), true)
    Zarr.writemetadata(Zarr.zarr_format(md), store, name, md)
    z[:] = data
    return z
end

# Shape/chunk notation below: (outer_chunk_shape_julia, inner_chunk_shape_julia)
# These are the reverses of Python's (shards, chunks) C-order tuples.

# 1d.contiguous.compressed.sharded.i2  shard=(4,) inner=(4,)
create_sharded(store, "1d.contiguous.compressed.sharded.i2",  Int16[1,2,3,4],              (4,), (4,))
# 1d.contiguous.compressed.sharded.i4
create_sharded(store, "1d.contiguous.compressed.sharded.i4",  Int32[1,2,3,4],              (4,), (4,))
# 1d.contiguous.compressed.sharded.u1
create_sharded(store, "1d.contiguous.compressed.sharded.u1",  UInt8[255,0,255,0],          (4,), (4,))
# 1d.contiguous.compressed.sharded.f4
create_sharded(store, "1d.contiguous.compressed.sharded.f4",  Float32[-1000.5,0,1000.5,0], (4,), (4,))
# 1d.contiguous.compressed.sharded.f8
create_sharded(store, "1d.contiguous.compressed.sharded.f8",  Float64[1.5,2.5,3.5,4.5],   (4,), (4,))
# 1d.contiguous.compressed.sharded.b1
create_sharded(store, "1d.contiguous.compressed.sharded.b1",  Bool[true,false,true,false], (4,), (4,))

# 1d.chunked.compressed.sharded.i2  shard=(2,) inner=(1,)
create_sharded(store, "1d.chunked.compressed.sharded.i2",          Int16[1,2,3,4],   (2,), (1,))
# 1d.chunked.filled.compressed.sharded.i2
create_sharded(store, "1d.chunked.filled.compressed.sharded.i2",   Int16[1,2,0,0],   (2,), (1,))

# 2d.contiguous.compressed.sharded.i2  shard=(2,2) inner=(2,2)
create_sharded(store, "2d.contiguous.compressed.sharded.i2",
    Int16[1 2; 3 4], (2,2), (2,2))

# 2d.chunked.compressed.sharded.filled.i2  shard=(2,2) inner=(1,1)
create_sharded(store, "2d.chunked.compressed.sharded.filled.i2",
    reshape(Int16.(0:15), 4, 4), (2,2), (1,1))

# 2d.chunked.compressed.sharded.i2  shard=(2,2) inner=(1,1)
create_sharded(store, "2d.chunked.compressed.sharded.i2",
    reshape(Int16.(1:16), 4, 4), (2,2), (1,1))

# 2d.chunked.ragged.compressed.sharded.i2  shard=(2,2) inner=(1,1)
create_sharded(store, "2d.chunked.ragged.compressed.sharded.i2",
    reshape(Int16.(1:9), 3, 3), (2,2), (1,1))

# 3d.contiguous.compressed.sharded.i2  shard=(3,3,3) inner=(3,3,3)
create_sharded(store, "3d.contiguous.compressed.sharded.i2",
    reshape(Int16.(0:26), 3, 3, 3), (3,3,3), (3,3,3))

# 3d.chunked.compressed.sharded.i2  shard=(2,2,2) inner=(1,1,1)
create_sharded(store, "3d.chunked.compressed.sharded.i2",
    reshape(Int16.(0:63), 4, 4, 4), (2,2,2), (1,1,1))

# 3d.chunked.mixed.compressed.sharded.i2
# Python: shards=(3,3,3) chunks=(3,3,1) → Julia: outer=(3,3,3) inner=(1,3,3)
create_sharded(store, "3d.chunked.mixed.compressed.sharded.i2",
    reshape(Int16.(0:26), 3, 3, 3), (3,3,3), (1,3,3))

# 1d.chunked.compressed.sharded.indexstart.i2  — exercises index_location=:start
# The :start branch has to re-encode the shard index after shifting chunk offsets,
# so it needs its own end-to-end fixture (Python-read and round-tripped).
create_sharded(store, "1d.chunked.compressed.sharded.indexstart.i2",
    Int16[10, 20, 30, 40], (2,), (1,); index_location=:start)

# 1d.chunked.compressed.sharded.noindexcrc.i2  — index_codecs = [bytes] only (no crc32c)
# Exercises compute_encoded_index_size on an index pipeline with no bytes→bytes codecs.
create_sharded(store, "1d.chunked.compressed.sharded.noindexcrc.i2",
    Int16[7, 14, 21, 28], (2,), (1,); index_crc32c=false)

# Group with spaces in the name
group_path = "my group with spaces"
zgroup(g, group_path; attrs=Dict("description" => "A group with spaces in the name"))

@info "Zarr v3 fixtures generated at: $path_v3"
