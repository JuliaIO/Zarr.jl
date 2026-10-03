# Julia script to generate Zarr v3 fixtures using PythonCall + CondaPkg
# Adapted from: https://github.com/manzt/zarrita.js/blob/23abb3bee9094aabbe60985626caef2802360963/scripts/generate-v3.py

using CondaPkg: CondaPkg, PkgSpec
using JSON

# Install Python deps into Conda env used by PythonCall (zarr v3 and numpy)
CondaPkg.add([
    PkgSpec("numpy"; version=">=2.3.3,<3"),
    PkgSpec("zarr"; version="3.*"),
    PkgSpec("numcodecs")
])

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
function create_and_fill(store; data, compressors = pylist([]), kw...)
    kwargs = filter(!isnothing ∘ last, kw)
    # create the array
    a = zarr.create_array(store; compressors, kwargs...)

    # ensure numpy array
    arr = data isa Py ? data : np.array(data)

    # assign content
    a.__setitem__(builtins.Ellipsis, arr)

    return a
end

# numpy dtype code -> zarr/numpy dtype name
dtype_lookup = Dict(
    "i2" => "int16",
    "i4" => "int32",
    "u1" => "uint8",
    "f2" => "float16",
    "f4" => "float32",
    "f8" => "float64",
    "b1" => "bool",
)

endian_lookup = Dict(
    "le" => "little",
    "be" => "big",
)

compressors_lookup = Dict(
    "gzip" => [codecs.GzipCodec()],
    "blosc" => [codecs.BloscCodec(typesize=4, shuffle="noshuffle")],
    "raw" => nothing,
)

# sample values for the 1d.contiguous.compressed.sharded.* examples, by dtype code
data_lookup = Dict(
    "i2" => [1, 2, 3, 4],
    "i4" => [1, 2, 3, 4],
    "u1" => [255, 0, 255, 0],
    "f4" => [-1000.5, 0, 1000.5, 0],
    "f8" => [1.5, 2.5, 3.5, 4.5],
    "b1" => [true, false, true, false],
)

# 1d.contiguous.{gzip,blosc,raw}.i2, 1d.contiguous.{gzip,blosc,raw}.string
for comp in ("gzip", "blosc", "raw")
    compressors = compressors_lookup[comp]
    # 1d.contiguous.$comp.i2
    create_and_fill(store;
        name="1d.contiguous.$comp.i2",
        dtype="int16",
        shape=(4,),
        chunks=(4,),
        serializer=codecs.BytesCodec(endian="little"),
        compressors,
        data=[1,2,3,4],
    )

    # 1d.contiguous.$comp.string
    create_and_fill(store;
        name="1d.contiguous.$comp.string",
        dtype="string",
        shape=(4,),
        chunks=(4,),
        compressors,
        data=["variable", "length", "utf8", "string"],
    )
end

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

# 1d.contiguous.f2.le, 1d.contiguous.f4.le, 1d.contiguous.f4.be
for (dtypepy, endianname) in zip(("f2", "f4", "f4"), ("le", "le", "be"))
    dtype = dtype_lookup[dtypepy]
    endian = endian_lookup[endianname]
    create_and_fill(store;
        name="1d.contiguous.$dtypepy.$endianname",
        dtype,
        shape=(4,),
        chunks=(4,),
        serializer=codecs.BytesCodec(; endian),
        compressors=[codecs.BloscCodec(typesize=4, shuffle="noshuffle")],
        data=np.array([-1000.5, 0.0, 1000.5, 0.0]; dtype),
    )
end

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
# 2d.contiguous.named.i2 -- v3 `dimension_names`, C order (null, "x") so that
# Julia reads ("x", nothing)
for (d, dimension_names) in zip(("", "named."), (nothing, (nothing, "x")))
    create_and_fill(store;
        name="2d.contiguous.$(d)i2",
        dtype="int16",
        shape=(2,2),
        chunks=(2,2),
        serializer=codecs.BytesCodec(endian="little"),
        compressors=[codecs.BloscCodec(typesize=4, shuffle="noshuffle")],
        data= np.array([ [1,2], [3,4] ] |> pylist, dtype="i2"),
        dimension_names,
    )
end

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

# 3d.contiguous.i2, 3d.chunked.i2, 3d.chunked.mixed.i2.C
for (name, chunks) in zip(
        ("3d.contiguous.i2", "3d.chunked.i2", "3d.chunked.mixed.i2.C"),
        ((3,3,3), (1,1,1), (3,3,1)),
    )
    create_and_fill(store;
        name,
        dtype="int16",
        shape=(3,3,3),
        chunks,
        serializer=codecs.BytesCodec(endian="little"),
        compressors=[codecs.BloscCodec(typesize=4, shuffle="noshuffle")],
        data=np.arange(27).reshape(3,3,3),
    )
end

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
# 1d.contiguous.compressed.sharded.{i2,i4,u1,f4,f8,b1}
for dtypepy in ("i2", "i4", "u1", "f4", "f8", "b1")
    dtype = dtype_lookup[dtypepy]
    # single-byte dtypes don't need an endianness-aware serializer
    serializer = dtypepy in ("u1", "b1") ? nothing : codecs.BytesCodec(endian="little")
    create_and_fill(store;
        name="1d.contiguous.compressed.sharded.$dtypepy",
        shape=(4,),
        dtype,
        chunks=(4,),
        shards=(4,),
        serializer,
        compressors=[codecs.GzipCodec()],
        data=np.array(data_lookup[dtypepy]; dtype),
    )
end

# 1d.chunked.compressed.sharded.i2, 1d.chunked.filled.compressed.sharded.i2
for (suffix, data) in (("", [1,2,3,4]), (".filled", [1,2,0,0]))
    create_and_fill(store;
        name="1d.chunked$suffix.compressed.sharded.i2",
        shape=(4,),
        dtype="int16",
        chunks=(1,),
        shards=(2,),
        serializer=codecs.BytesCodec(endian="little"),
        compressors=[codecs.GzipCodec()],
        data=np.array(data; dtype="i2"),
    )
end

# 2d.contiguous.compressed.sharded.i2, 2d.chunked.compressed.sharded.filled.i2,
# 2d.chunked.compressed.sharded.i2, 2d.chunked.ragged.compressed.sharded.i2
for (name, shape, chunks, data) in (
        ("2d.contiguous.compressed.sharded.i2", (2,2), (2,2), np.arange(1,5; dtype="i2").reshape(2,2)),
        ("2d.chunked.compressed.sharded.filled.i2", (4,4), (1,1), np.arange(16; dtype="i2").reshape(4,4)),
        ("2d.chunked.compressed.sharded.i2", (4,4), (1,1), np.arange(16; dtype="i2").reshape(4,4) + 1),
        ("2d.chunked.ragged.compressed.sharded.i2", (3,3), (1,1), np.arange(1,10; dtype="i2").reshape(3,3)),
    )
    create_and_fill(store;
        name,
        shape,
        dtype="int16",
        chunks,
        shards=(2,2),
        serializer=codecs.BytesCodec(endian="little"),
        compressors=[codecs.GzipCodec()],
        data,
    )
end

# 3d.contiguous.compressed.sharded.i2, 3d.chunked.compressed.sharded.i2,
# 3d.chunked.mixed.compressed.sharded.i2
for (name, shape, chunks, shards, n) in (
        ("3d.contiguous.compressed.sharded.i2", (3,3,3), (3,3,3), (3,3,3), 27),
        ("3d.chunked.compressed.sharded.i2", (4,4,4), (1,1,1), (2,2,2), 64),
        ("3d.chunked.mixed.compressed.sharded.i2", (3,3,3), (3,3,1), (3,3,3), 27),
    )
    create_and_fill(store;
        name,
        shape,
        dtype="int16",
        chunks,
        shards,
        serializer=codecs.BytesCodec(endian="little"),
        compressors=[codecs.GzipCodec()],
        data=np.arange(n; dtype="i2").reshape(shape),
    )
end

# Group with spaces in the name
g = zarr.create_group(store, path="my group with spaces")
g.attrs["description"] = "A group with spaces in the name"

# Consolidated group and a subgroup with its own consolidated metadata
consolidated = zarr.create_group(store, path="consolidated")
consolidated.attrs["answer"] = 42

# consolidated/1d.chunked.i2
create_and_fill(store;
    name="consolidated/1d.chunked.i2",
    dtype="int16",
    shape=(4,),
    chunks=(2,),
    serializer=codecs.BytesCodec(endian="little"),
    compressors=nothing,
    data=np.array([1,2,3,4], dtype="i2"),
)

# consolidated/2d.contiguous.i2
create_and_fill(store;
    name="consolidated/2d.contiguous.i2",
    dtype="int16",
    shape=(2,2),
    chunks=(2,2),
    serializer=codecs.BytesCodec(endian="little"),
    compressors=nothing,
    data=np.array([[1,2],[3,4]] |> pylist, dtype="i2"),
)

# consolidated/nested group
nested = zarr.create_group(store, path="consolidated/nested")
nested.attrs.__setitem__("description", "A nested group")

# consolidated/nested/1d.i2
create_and_fill(store;
    name="consolidated/nested/1d.i2",
    dtype="int16",
    shape=(4,),
    chunks=(4,),
    serializer=codecs.BytesCodec(endian="little"),
    compressors=nothing,
    data=np.array([10,20,30,40], dtype="i2"),
)

# Consolidate metadata for the consolidated group
zarr.consolidate_metadata(store, path="consolidated")

@info "Zarr v3 fixtures generated at: $path_v3"
