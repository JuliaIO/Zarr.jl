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
store = Zarr.FormattedStore{3, '/'}(Zarr.DirectoryStore(path_v3))
# Manually create v3 group metadata (zgroup defaults to v2) # TODO: we need to fix this!
group_meta = Dict("zarr_format" => 3, "node_type" => "group")
b = IOBuffer()
JSON.print(b, group_meta)
store["", "zarr.json"] = take!(b)

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
# Note: Sharding is not yet fully implemented in Zarr.jl, so these examples
# may not produce the exact same structure as the Python version.
# They are included for completeness but may need adjustment once sharding is supported.

# 1d.contiguous.compressed.sharded.i2
create_and_fill(store, "1d.contiguous.compressed.sharded.i2", Int16[1,2,3,4];
    shape=(4,),
    chunks=(4,),
    compressor=Zarr.ZlibCompressor(),
)

# 1d.contiguous.compressed.sharded.i4
create_and_fill(store, "1d.contiguous.compressed.sharded.i4", Int32[1,2,3,4];
    shape=(4,),
    chunks=(4,),
    compressor=Zarr.ZlibCompressor(),
)

# 1d.contiguous.compressed.sharded.u1
create_and_fill(store, "1d.contiguous.compressed.sharded.u1", UInt8[255,0,255,0];
    shape=(4,),
    chunks=(4,),
    compressor=Zarr.ZlibCompressor(),
)

# 1d.contiguous.compressed.sharded.f4
create_and_fill(store, "1d.contiguous.compressed.sharded.f4", Float32[-1000.5,0,1000.5,0];
    shape=(4,),
    chunks=(4,),
    compressor=Zarr.ZlibCompressor(),
)

# 1d.contiguous.compressed.sharded.f8
create_and_fill(store, "1d.contiguous.compressed.sharded.f8", Float64[1.5,2.5,3.5,4.5];
    shape=(4,),
    chunks=(4,),
    compressor=Zarr.ZlibCompressor(),
)

# 1d.contiguous.compressed.sharded.b1
create_and_fill(store, "1d.contiguous.compressed.sharded.b1", Bool[true,false,true,false];
    shape=(4,),
    chunks=(4,),
    compressor=Zarr.ZlibCompressor(),
)

# 1d.chunked.compressed.sharded.i2
create_and_fill(store, "1d.chunked.compressed.sharded.i2", Int16[1,2,3,4];
    shape=(4,),
    chunks=(1,),
    compressor=Zarr.ZlibCompressor(),
)

# 1d.chunked.filled.compressed.sharded.i2
create_and_fill(store, "1d.chunked.filled.compressed.sharded.i2", Int16[1,2,0,0];
    shape=(4,),
    chunks=(1,),
    compressor=Zarr.ZlibCompressor(),
)

# 2d.contiguous.compressed.sharded.i2
create_and_fill(store, "2d.contiguous.compressed.sharded.i2", Int16[1 2; 3 4];
    shape=(2,2),
    chunks=(2,2),
    compressor=Zarr.ZlibCompressor(),
)

# 2d.chunked.compressed.sharded.filled.i2
create_and_fill(store, "2d.chunked.compressed.sharded.filled.i2", reshape(Int16.(0:15), 4, 4);
    shape=(4,4),
    chunks=(1,1),
    compressor=Zarr.ZlibCompressor(),
)

# 2d.chunked.compressed.sharded.i2
create_and_fill(store, "2d.chunked.compressed.sharded.i2", reshape(Int16.(1:16), 4, 4);
    shape=(4,4),
    chunks=(1,1),
    compressor=Zarr.ZlibCompressor(),
)

# 2d.chunked.ragged.compressed.sharded.i2
create_and_fill(store, "2d.chunked.ragged.compressed.sharded.i2", reshape(Int16.(1:9), 3, 3);
    shape=(3,3),
    chunks=(1,1),
    compressor=Zarr.ZlibCompressor(),
)

# 3d.contiguous.compressed.sharded.i2
create_and_fill(store, "3d.contiguous.compressed.sharded.i2", reshape(Int16.(0:26), 3, 3, 3);
    shape=(3,3,3),
    chunks=(3,3,3),
    compressor=Zarr.ZlibCompressor(),
)

# 3d.chunked.compressed.sharded.i2
create_and_fill(store, "3d.chunked.compressed.sharded.i2", reshape(Int16.(0:63), 4, 4, 4);
    shape=(4,4,4),
    chunks=(1,1,1),
    compressor=Zarr.ZlibCompressor(),
)

# 3d.chunked.mixed.compressed.sharded.i2
create_and_fill(store, "3d.chunked.mixed.compressed.sharded.i2", reshape(Int16.(0:26), 3, 3, 3);
    shape=(3,3,3),
    chunks=(3,3,1),
    compressor=Zarr.ZlibCompressor(),
)

# Group with spaces in the name
group_path = "my group with spaces"
group_meta2 = Dict("zarr_format" => 3, "node_type" => "group", "attributes" => Dict("description" => "A group with spaces in the name"))
b2 = IOBuffer()
JSON.print(b2, group_meta2)
store[group_path, "zarr.json"] = take!(b2)

@info "Zarr v3 fixtures generated at: $path_v3"