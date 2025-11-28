# Julia script to generate Zarr v3 fixtures using PythonCall + CondaPkg
# Adapted from: https://github.com/manzt/zarrita.js/blob/23abb3bee9094aabbe60985626caef2802360963/scripts/generate-v3.py

using CondaPkg
using JSON

# Install Python deps into Conda env used by PythonCall (zarr v3 and numpy)
CondaPkg.add("numpy")
CondaPkg.add("zarr"; version="3.*")
CondaPkg.add("numcodecs")

using PythonCall
# Import Python modules
np = pyimport("numpy")
zarr = pyimport("zarr")
codecs = pyimport("zarr.codecs")
storage = pyimport("zarr.storage")
json = pyimport("json")
shutil = pyimport("shutil")
pathlib = pyimport("pathlib")
builtins = pyimport("builtins")

# Paths
path_v3 = joinpath(@__DIR__, "v3_python", "data.zarr")

# deterministic RNG for numpy
np.random.seed(42)

# remove existing
try
    shutil.rmtree(path_v3)
catch
    # ignore
end

# create store and path_v3 group
store = storage.LocalStore(path_v3)
zarr.create_group(store)

# helper: create array and set data (value should be a numpy array or convertible)
function create_and_fill(store; name, dtype=nothing, shape=nothing, chunks=nothing,
    serializer=nothing, compressors=nothing, filters=nothing, shards=nothing, data)
    # Build NamedTuple of only non-nothing keyword arguments
    kwargs = (; name=name)
    if dtype !== nothing
        kwargs = merge(kwargs, (; dtype=dtype))
    end
    if shape !== nothing
        kwargs = merge(kwargs, (; shape=shape))
    end
    if chunks !== nothing
        kwargs = merge(kwargs, (; chunks=chunks))
    end
    if serializer !== nothing
        kwargs = merge(kwargs, (; serializer=serializer))
    end
    if compressors !== nothing
        kwargs = merge(kwargs, (; compressors=compressors))
    end
    if filters !== nothing
        kwargs = merge(kwargs, (; filters=filters))
    end
    if shards !== nothing
        kwargs = merge(kwargs, (; shards=shards))
    end

    # create the array
    a = zarr.create_array(store; kwargs...)

    # ensure numpy array
    arr = data isa Py ? data : np.array(data)

    # assign content
    a.__setitem__(builtins.Ellipsis, arr)

    return a
end

# 1d.contiguous.gzip.i2
create_and_fill(store;
    name="1d.contiguous.gzip.i2",
    dtype="int16",
    shape=(4,),
    chunks=(4,),
    serializer=codecs.BytesCodec(endian="little"),
    compressors=[codecs.GzipCodec()],
    data=[1,2,3,4],
)

# 1d.contiguous.blosc.i2
create_and_fill(store;
    name="1d.contiguous.blosc.i2",
    dtype="int16",
    shape=(4,),
    chunks=(4,),
    serializer=codecs.BytesCodec(endian="little"),
    compressors=[codecs.BloscCodec(typesize=4, shuffle="noshuffle")],
    data=[1,2,3,4],
)

# 1d.contiguous.raw.i2
create_and_fill(store;
    name="1d.contiguous.raw.i2",
    dtype="int16",
    shape=(4,),
    chunks=(4,),
    serializer=codecs.BytesCodec(endian="little"),
    compressors=nothing,
    data=[1,2,3,4],
)

# 1d.contiguous.i4
create_and_fill(store;
    name="1d.contiguous.i4",
    dtype="int32",
    shape=(4,),
    chunks=(4,),
    serializer=codecs.BytesCodec(endian="little"),
    compressors=[codecs.BloscCodec(typesize=4, shuffle="noshuffle")],
    data=[1,2,3,4],
)

# 1d.contiguous.u1
create_and_fill(store;
    name="1d.contiguous.u1",
    dtype="uint8",
    shape=(4,),
    chunks=(4,),
    compressors=[codecs.BloscCodec(typesize=4, shuffle="noshuffle")],
    data=np.array([255,0,255,0], dtype="u1")
)

# 1d.contiguous.f2.le
create_and_fill(store;
    name="1d.contiguous.f2.le",
    dtype="float16",
    shape=(4,),
    chunks=(4,),
    serializer=codecs.BytesCodec(endian="little"),
    compressors=[codecs.BloscCodec(typesize=4, shuffle="noshuffle")],
    data=np.array([-1000.5, 0.0, 1000.5, 0.0], dtype="f2"),
)

# 1d.contiguous.f4.le
create_and_fill(store;
    name="1d.contiguous.f4.le",
    dtype="float32",
    shape=(4,),
    chunks=(4,),
    serializer=codecs.BytesCodec(endian="little"),
    compressors=[codecs.BloscCodec(typesize=4, shuffle="noshuffle")],
    data=np.array([-1000.5, 0.0, 1000.5, 0.0], dtype="f4"),
)

# 1d.contiguous.f4.be
create_and_fill(store;
    name="1d.contiguous.f4.be",
    dtype="float32",
    shape=(4,),
    chunks=(4,),
    serializer=codecs.BytesCodec(endian="big"),
    compressors=[codecs.BloscCodec(typesize=4, shuffle="noshuffle")],
    data=np.array([-1000.5, 0.0, 1000.5, 0.0], dtype="f4"),
)

# 1d.contiguous.f8
create_and_fill(store;
    name="1d.contiguous.f8",
    dtype="float64",
    shape=(4,),
    chunks=(4,),
    serializer=codecs.BytesCodec(endian="little"),
    compressors=[codecs.BloscCodec(typesize=4, shuffle="noshuffle")],
    data=np.array([1.5,2.5,3.5,4.5], dtype="f8"),
)

# 1d.contiguous.b1
create_and_fill(store;
    name="1d.contiguous.b1",
    dtype="bool",
    shape=(4,),
    chunks=(4,),
    compressors=[codecs.BloscCodec(typesize=4, shuffle="noshuffle")],
    data=np.array([true,false,true,false], dtype="bool"),
)

# 1d.chunked.i2
create_and_fill(store;
    name="1d.chunked.i2",
    dtype="int16",
    shape=(4,),
    chunks=(2,),
    serializer=codecs.BytesCodec(endian="little"),
    compressors=[codecs.BloscCodec(typesize=4, shuffle="noshuffle")],
    data=np.array([1,2,3,4], dtype="i2"),
)

# adjust zarr.json to set dimension_names = null
meta_path = joinpath(path_v3, "1d.chunked.i2", "zarr.json")
meta = JSON.parsefile(meta_path; dicttype = Dict{String,Any})
meta["dimension_names"] = nothing
open(meta_path, "w") do io
    JSON.print(io, meta)
end

# 1d.chunked.ragged.i2
create_and_fill(store;
    name="1d.chunked.ragged.i2",
    dtype="int16",
    shape=(5,),
    chunks=(2,),
    serializer=codecs.BytesCodec(endian="little"),
    compressors=[codecs.BloscCodec(typesize=4, shuffle="noshuffle")],
    data=np.array([1,2,3,4,5], dtype="i2"),
)

# 2d.contiguous.i2
create_and_fill(store;
    name="2d.contiguous.i2",
    dtype="int16",
    shape=(2,2),
    chunks=(2,2),
    serializer=codecs.BytesCodec(endian="little"),
    compressors=[codecs.BloscCodec(typesize=4, shuffle="noshuffle")],
    data= np.array([ [1,2], [3,4] ] |> pylist, dtype="i2"),
)

# 2d.chunked.i2
create_and_fill(store;
    name="2d.chunked.i2",
    dtype="int16",
    shape=(2,2),
    chunks=(1,1),
    serializer=codecs.BytesCodec(endian="little"),
    compressors=[codecs.BloscCodec(typesize=4, shuffle="noshuffle")],
    data=np.array([[1,2],[3,4]] |> pylist, dtype="i2"),
)

# 2d.chunked.ragged.i2
create_and_fill(store;
    name="2d.chunked.ragged.i2",
    dtype="int16",
    shape=(3,3),
    chunks=(2,2),
    serializer=codecs.BytesCodec(endian="little"),
    compressors=[codecs.BloscCodec(typesize=4, shuffle="noshuffle")],
    data=np.array([[1,2,3],[4,5,6],[7,8,9]] |> pylist, dtype="i2"),
)

# 3d.contiguous.i2
create_and_fill(store;
    name="3d.contiguous.i2",
    dtype="int16",
    shape=(3,3,3),
    chunks=(3,3,3),
    serializer=codecs.BytesCodec(endian="little"),
    compressors=[codecs.BloscCodec(typesize=4, shuffle="noshuffle")],
    data=np.arange(27).reshape(3,3,3),
)

# 3d.chunked.i2
create_and_fill(store;
    name="3d.chunked.i2",
    dtype="int16",
    shape=(3,3,3),
    chunks=(1,1,1),
    serializer=codecs.BytesCodec(endian="little"),
    compressors=[codecs.BloscCodec(typesize=4, shuffle="noshuffle")],
    data=np.arange(27).reshape(3,3,3),
)

# 3d.chunked.mixed.i2.C
create_and_fill(store;
    name="3d.chunked.mixed.i2.C",
    dtype="int16",
    shape=(3,3,3),
    chunks=(3,3,1),
    serializer=codecs.BytesCodec(endian="little"),
    compressors=[codecs.BloscCodec(typesize=4, shuffle="noshuffle")],
    data=np.arange(27).reshape(3,3,3),
)

# 3d.chunked.mixed.i2.F  (with transpose filter to simulate column-major)
transpose_filter = codecs.TransposeCodec(order=[2,1,0])
create_and_fill(store;
    name="3d.chunked.mixed.i2.F",
    dtype="int16",
    shape=(3,3,3),
    chunks=(3,3,1),
    filters=[transpose_filter],
    serializer=codecs.BytesCodec(endian="little"),
    compressors=[codecs.BloscCodec(typesize=4, shuffle="noshuffle")],
    data=np.arange(27).reshape(3,3,3),
)

##### Sharded/compressed examples
# 1d.contiguous.compressed.sharded.i2
create_and_fill(store;
    name="1d.contiguous.compressed.sharded.i2",
    shape=(4,),
    dtype=np.array([1,2,3,4], dtype="i2").dtype,
    chunks=(4,),
    shards=(4,),
    serializer=codecs.BytesCodec(endian="little"),
    compressors=[codecs.GzipCodec()],
    data=np.array([1,2,3,4], dtype="i2"),
)

# 1d.contiguous.compressed.sharded.i4
create_and_fill(store;
    name="1d.contiguous.compressed.sharded.i4",
    shape=(4,),
    dtype=np.array([1,2,3,4], dtype="i4").dtype,
    chunks=(4,),
    shards=(4,),
    serializer=codecs.BytesCodec(endian="little"),
    compressors=[codecs.GzipCodec()],
    data=np.array([1,2,3,4], dtype="i4"),
)

# 1d.contiguous.compressed.sharded.u1
create_and_fill(store;
    name="1d.contiguous.compressed.sharded.u1",
    shape=(4,),
    dtype=np.array([255,0,255,0], dtype="u1").dtype,
    chunks=(4,),
    shards=(4,),
    compressors=[codecs.GzipCodec()],
    data=np.array([255,0,255,0], dtype="u1"),
)

# 1d.contiguous.compressed.sharded.f4
create_and_fill(store;
    name="1d.contiguous.compressed.sharded.f4",
    shape=(4,),
    dtype=np.array([-1000.5,0,1000.5,0], dtype="f4").dtype,
    chunks=(4,),
    shards=(4,),
    serializer=codecs.BytesCodec(endian="little"),
    compressors=[codecs.GzipCodec()],
    data=np.array([-1000.5,0,1000.5,0], dtype="f4"),
)

# 1d.contiguous.compressed.sharded.f8
create_and_fill(store;
    name="1d.contiguous.compressed.sharded.f8",
    shape=(4,),
    dtype=np.array([1.5,2.5,3.5,4.5], dtype="f8").dtype,
    chunks=(4,),
    shards=(4,),
    serializer=codecs.BytesCodec(endian="little"),
    compressors=[codecs.GzipCodec()],
    data=np.array([1.5,2.5,3.5,4.5], dtype="f8"),
)

# 1d.contiguous.compressed.sharded.b1
create_and_fill(store;
    name="1d.contiguous.compressed.sharded.b1",
    shape=(4,),
    dtype="bool",
    chunks=(4,),
    shards=(4,),
    compressors=[codecs.GzipCodec()],
    data=np.array([true,false,true,false], dtype="bool"),
)

# 1d.chunked.compressed.sharded.i2
create_and_fill(store;
    name="1d.chunked.compressed.sharded.i2",
    shape=(4,),
    dtype=np.array([1,2,3,4], dtype="i2").dtype,
    chunks=(1,),
    shards=(2,),
    serializer=codecs.BytesCodec(endian="little"),
    compressors=[codecs.GzipCodec()],
    data=np.array([1,2,3,4], dtype="i2"),
)

# 1d.chunked.filled.compressed.sharded.i2
create_and_fill(store;
    name="1d.chunked.filled.compressed.sharded.i2",
    shape=(4,),
    dtype=np.array([1,2,0,0], dtype="i2").dtype,
    chunks=(1,),
    shards=(2,),
    serializer=codecs.BytesCodec(endian="little"),
    compressors=[codecs.GzipCodec()],
    data=np.array([1,2,0,0], dtype="i2"),
)

# 2d.contiguous.compressed.sharded.i2
create_and_fill(store;
    name="2d.contiguous.compressed.sharded.i2",
    shape=(2,2),
    dtype=np.arange(1,5, dtype="i2").dtype,
    chunks=(2,2),
    shards=(2,2),
    serializer=codecs.BytesCodec(endian="little"),
    compressors=[codecs.GzipCodec()],
    data=np.arange(1,5, dtype="i2").reshape(2,2),
)

# 2d.chunked.compressed.sharded.filled.i2
create_and_fill(store;
    name="2d.chunked.compressed.sharded.filled.i2",
    shape=(4,4),
    dtype=np.arange(16, dtype="i2").dtype,
    chunks=(1,1),
    shards=(2,2),
    serializer=codecs.BytesCodec(endian="little"),
    compressors=[codecs.GzipCodec()],
    data=np.arange(16, dtype="i2").reshape(4,4),
)

# 2d.chunked.compressed.sharded.i2
create_and_fill(store;
    name="2d.chunked.compressed.sharded.i2",
    shape=(4,4),
    dtype=np.arange(16, dtype="i2").dtype,
    chunks=(1,1),
    shards=(2,2),
    serializer=codecs.BytesCodec(endian="little"),
    compressors=[codecs.GzipCodec()],
    data=(np.arange(16, dtype="i2").reshape(4,4) + 1),
)

# 2d.chunked.ragged.compressed.sharded.i2
create_and_fill(store;
    name="2d.chunked.ragged.compressed.sharded.i2",
    shape=(3,3),
    dtype=np.arange(1,10, dtype="i2").dtype,
    chunks=(1,1),
    shards=(2,2),
    serializer=codecs.BytesCodec(endian="little"),
    compressors=[codecs.GzipCodec()],
    data=np.arange(1,10, dtype="i2").reshape(3,3),
)

# 3d.contiguous.compressed.sharded.i2
create_and_fill(store;
    name="3d.contiguous.compressed.sharded.i2",
    shape=(3,3,3),
    dtype=np.arange(27, dtype="i2").dtype,
    chunks=(3,3,3),
    shards=(3,3,3),
    serializer=codecs.BytesCodec(endian="little"),
    compressors=[codecs.GzipCodec()],
    data=np.arange(27, dtype="i2").reshape(3,3,3),
)

# 3d.chunked.compressed.sharded.i2
create_and_fill(store;
    name="3d.chunked.compressed.sharded.i2",
    shape=(4,4,4),
    dtype=np.arange(64, dtype="i2").dtype,
    chunks=(1,1,1),
    shards=(2,2,2),
    serializer=codecs.BytesCodec(endian="little"),
    compressors=[codecs.GzipCodec()],
    data=np.arange(64, dtype="i2").reshape(4,4,4),
)

# 3d.chunked.mixed.compressed.sharded.i2
create_and_fill(store;
    name="3d.chunked.mixed.compressed.sharded.i2",
    shape=(3,3,3),
    dtype=np.arange(27, dtype="i2").dtype,
    chunks=(3,3,1),
    shards=(3,3,3),
    serializer=codecs.BytesCodec(endian="little"),
    compressors=[codecs.GzipCodec()],
    data=np.arange(27, dtype="i2").reshape(3,3,3),
)

# Group with spaces in the name
g = zarr.create_group(store, path="my group with spaces")
g.attrs["description"] = "A group with spaces in the name"

@info "Zarr v3 fixtures generated at: $path_v3"
