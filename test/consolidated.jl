using CondaPkg: CondaPkg, PkgSpec
using JSON
using PythonCall

CondaPkg.add([
    PkgSpec("numpy"),
    PkgSpec("zarr"; version="3.*"),
    PkgSpec("numcodecs")
])

pyzarr = pyimport("zarr")
pystorage = pyimport("zarr.storage")
pyjson = pyimport("json")

path_v3_python = joinpath(@__DIR__, "v3_python", "data.zarr")
path_v3_julia = joinpath(@__DIR__, "v3_julia", "data.zarr")

@testset "Consolidated Metadata" begin

  @testset "round-trip Julia vs Python consolidated metadata" begin
    path_py = joinpath(path_v3_python, "consolidated")
    path_jl = joinpath(path_v3_julia, "consolidated")

    # Read Python-written store with Python
    store_py_from_py = pystorage.LocalStore(path_py)
    group_py_from_py = pyzarr.open_consolidated(store_py_from_py)
    py_from_py_str = pyconvert(String, pyjson.dumps(group_py_from_py.metadata.consolidated_metadata.to_dict()))
    py_from_py = JSON.parse(py_from_py_str)

    # Read Julia-written store with Python
    store_py_from_jl = pystorage.LocalStore(path_jl)
    group_py_from_jl = pyzarr.open_consolidated(store_py_from_jl)
    py_from_jl_str = pyconvert(String, pyjson.dumps(group_py_from_jl.metadata.consolidated_metadata.to_dict()))
    py_from_jl = JSON.parse(py_from_jl_str)

    # Read Python-written store with Julia
    consolidated_group_jl_from_py = zopen(path_py, consolidated=true)
    jl_from_py = JSON.parse(JSON.json(consolidated_group_jl_from_py.storage.cons))

    # Read Julia-written store with Julia
    consolidated_group_jl_from_jl = zopen(path_jl, consolidated=true)
    jl_from_jl = JSON.parse(JSON.json(consolidated_group_jl_from_jl.storage.cons))

    @testset "Python-written, read by Python vs Julia" begin
      @test py_from_py == jl_from_py
    end
    @testset "Julia-written, read by Python vs Julia" begin
      @test py_from_jl == jl_from_jl
    end
    @testset "Python-written vs Julia-written, read by Python" begin
      @test py_from_py == py_from_jl
    end
    @testset "Python-written vs Julia-written, read by Julia" begin
      @test jl_from_py == jl_from_jl
    end
  end

  @testset "v2 consolidate_metadata writes .zmetadata" begin
    s = Zarr.DictStore()
    g = zgroup(s, attrs=Dict("root" => 1))
    zcreate(Int, g, "arr", 10, chunks=(5,), attrs=Dict("x" => 2))
    sub = zgroup(g, "sub", attrs=Dict("y" => 3))
    zcreate(Float32, sub, "nested", 4, chunks=(2,))

    cs = Zarr.consolidate_metadata(s, "")
    @test cs isa Zarr.ConsolidatedStore

    raw = s.a[".zmetadata"]
    @test raw !== nothing
    parsed = JSON.parse(String(copy(raw)))
    m = parsed["metadata"]
    @test haskey(m, ".zgroup")
    @test haskey(m, ".zattrs")           # root attrs
    @test haskey(m, "arr/.zarray")
    @test haskey(m, "arr/.zattrs")
    @test haskey(m, "sub/.zgroup")
    @test haskey(m, "sub/nested/.zarray")
    @test parsed["zarr_consolidated_format"] == 1
    # cons dict is the metadata subdict, not the whole root
    @test cs.cons === nothing || cs.cons isa Dict
    @test haskey(cs.cons, ".zgroup")
  end

  @testset "v2 ConsolidatedStore auto-detect constructor" begin
    s = Zarr.DictStore()
    g = zgroup(s)
    Zarr.consolidate_metadata(s, "")
    # auto-detect: reads .zmetadata, detects v2
    cs = Zarr.ConsolidatedStore(s, "")
    @test cs isa Zarr.ConsolidatedStore
  end

  @testset "v2 ConsolidatedStore constructor errors" begin
    # Missing .zmetadata
    s = Zarr.DictStore()
    zgroup(s)
    @test_throws ArgumentError Zarr.ConsolidatedStore(s, "", Zarr.ZarrFormat(2))

    # .zmetadata present but missing "metadata" field
    s2 = Zarr.DictStore()
    s2[".zmetadata"] = Vector{UInt8}("""{"zarr_consolidated_format":1}""")
    @test_throws ArgumentError Zarr.ConsolidatedStore(s2, "", Zarr.ZarrFormat(2))
  end

  @testset "v3 ConsolidatedStore constructor errors" begin
    # Missing zarr.json
    s = Zarr.DictStore()
    @test_throws ArgumentError Zarr.ConsolidatedStore(s, "", Zarr.ZarrFormat(3))

    # zarr.json present but no consolidated_metadata
    s2 = Zarr.DictStore()
    s2["zarr.json"] = Vector{UInt8}("""{"zarr_format":3,"node_type":"group"}""")
    @test_throws ArgumentError Zarr.ConsolidatedStore(s2, "", Zarr.ZarrFormat(3))

    # consolidated_metadata present but no metadata subkey
    s3 = Zarr.DictStore()
    s3["zarr.json"] = Vector{UInt8}("""{"zarr_format":3,"consolidated_metadata":{"kind":"inline"}}""")
    @test_throws ArgumentError Zarr.ConsolidatedStore(s3, "", Zarr.ZarrFormat(3))
  end

  @testset "auto-detect constructor errors when no format found" begin
    s = Zarr.DictStore()   # empty store, no .zmetadata or zarr.json
    @test_throws ArgumentError Zarr.ConsolidatedStore(s, "")
  end

  @testset "v2 getattrs" begin
    s = Zarr.DictStore()
    g = zgroup(s, attrs=Dict("a" => 1))
    zcreate(Int, g, "arr", 4, chunks=(2,), attrs=Dict("b" => 2))
    zgroup(g, "sub")   # group with no attrs
    cs = Zarr.consolidate_metadata(s, "")
    V2 = Zarr.ZarrFormat(2)
    @test Zarr.getattrs(V2, cs, "") == Dict("a" => 1)
    @test Zarr.getattrs(V2, cs, "arr") == Dict("b" => 2)
    # No .zattrs written for sub → returns empty dict
    @test Zarr.getattrs(V2, cs, "sub") == Dict{String,Any}()
    @test Zarr.getattrs(V2, cs, "nonexistent") == Dict{String,Any}()
  end

  @testset "v2 is_zarray / is_zgroup" begin
    s = Zarr.DictStore()
    g = zgroup(s)
    zgroup(g, "sub")
    zcreate(Float64, g, "data", 8, chunks=(4,))
    cs = Zarr.consolidate_metadata(s, "")
    V2 = Zarr.ZarrFormat(2)
    @test  Zarr.is_zgroup(V2, cs, "")
    @test !Zarr.is_zarray(V2, cs, "")
    @test  Zarr.is_zgroup(V2, cs, "sub")
    @test !Zarr.is_zarray(V2, cs, "sub")
    @test  Zarr.is_zarray(V2, cs, "data")
    @test !Zarr.is_zgroup(V2, cs, "data")
    @test !Zarr.is_zarray(V2, cs, "nonexistent")
    @test !Zarr.is_zgroup(V2, cs, "nonexistent")
  end

  @testset "v2 subdirs" begin
    s = Zarr.DictStore()
    g = zgroup(s)
    zgroup(g, "child1")
    zgroup(g, "child2")
    zcreate(Int, g, "arr", 4, chunks=(2,))
    cs = Zarr.consolidate_metadata(s, "")
    dirs = Zarr.subdirs(cs, "")
    @test sort(dirs) == ["arr", "child1", "child2"]
    @test Zarr.subdirs(cs, "child1") == String[]
  end

  @testset "v2 getmetadata" begin
    s = Zarr.DictStore()
    g = zgroup(s)
    a = zcreate(Int32, g, "arr", 10, 20, chunks=(5,5), fill_value=Int32(-1))
    # I think this `fill_value` way of passing things is related to issue: https://github.com/JuliaIO/Zarr.jl/issues/292
    cs = Zarr.consolidate_metadata(s, "")
    V2 = Zarr.ZarrFormat(2)
    meta = Zarr.getmetadata(V2, cs, "arr", false)
    @test meta.dtype == Int32 || eltype(meta) == Int32
    @test meta.chunks == (5, 5)
  end

  @testset "v3 getattrs" begin
    # canonical: attributes under "attributes" subkey
    cons = Dict{String,Any}(
      "zarr.json" => Dict{String,Any}(
        "node_type" => "group", "zarr_format" => 3,
        "attributes" => Dict{String,Any}("foo" => "bar")
      )
    )
    cs = Zarr.ConsolidatedStore(Zarr.DictStore(), "", cons)
    @test Zarr.getattrs(Zarr.ZarrFormat(3), cs, "") == Dict("foo" => "bar")

    # zarr.json present but no "attributes"
    cs2 = Zarr.ConsolidatedStore(Zarr.DictStore(), "",
      Dict{String,Any}("zarr.json" => Dict{String,Any}("node_type" => "group")))
    @test Zarr.getattrs(Zarr.ZarrFormat(3), cs2, "") == Dict{String,Any}()

    # zarr.json key absent entirely
    cs3 = Zarr.ConsolidatedStore(Zarr.DictStore(), "", Dict{String,Any}())
    @test Zarr.getattrs(Zarr.ZarrFormat(3), cs3, "") == Dict{String,Any}()
  end

  @testset "v3 is_zarray / is_zgroup" begin
    # cons for v3 stores cm dict; cm["metadata"] holds node entries keyed by path
    # is_zgroup(V3, cs, p) computes key = _unconcpath(d, p, "zarr.json")
    # is_zarray(V3, cs, p) computes key = _unconcpath(d, p)  (no suffix)
    meta = Dict{String,Any}(
      "group1/zarr.json" => Dict{String,Any}("node_type" => "group"),
      "group1/arr" => Dict{String,Any}("node_type" => "array"),
    )
    cons = Dict{String,Any}("metadata" => meta)
    s = Zarr.DictStore()
    s["zarr.json"] = Vector{UInt8}("""{"zarr_format":3,"node_type":"group"}""")
    cs = Zarr.ConsolidatedStore(s, "", cons)
    V3 = Zarr.ZarrFormat(3)
    # is_zarray looks up _unconcpath(d, p) — no suffix — so p="group1/arr"
    @test  Zarr.is_zarray(V3, cs, "group1/arr")
    @test !Zarr.is_zarray(V3, cs, "group1")      # group1 has no bare key in metadata
    @test !Zarr.is_zarray(V3, cs, "nonexistent")
    # is_zgroup looks up _unconcpath(d, p, "zarr.json") so p="group1"
    @test  Zarr.is_zgroup(V3, cs, "group1")
    @test !Zarr.is_zgroup(V3, cs, "group1/arr")  # arr is not a group
    @test !Zarr.is_zgroup(V3, cs, "nonexistent")
  end

  @testset "write protection" begin
    s = Zarr.DictStore()
    g = zgroup(s)
    cs = Zarr.consolidate_metadata(s, "")
    for key in (".zgroup", "arr/.zarray", "arr/.zattrs")
      @test_throws ArgumentError cs[key] = rand(UInt8, 10)
      @test_throws ArgumentError delete!(cs, key)
    end
    # Chunk data writes are allowed
    @test_nowarn (cs["arr/0"] = rand(UInt8, 10))
    @test_nowarn delete!(cs, "arr/0")
  end

  @testset "show" begin
    s = Zarr.DictStore()
    zgroup(s)
    cs = Zarr.consolidate_metadata(s, "")
    shown = sprint(show, cs)
    @test startswith(shown, "Consolidated ")
    @test occursin("Dict", shown)
  end

  @testset "storagesize and getindex delegate to parent" begin
    s = Zarr.DictStore()
    g = zgroup(s)
    a = zcreate(Int, g, "arr", 10, chunks=(5,))
    a .= 1:10
    cs = Zarr.consolidate_metadata(s, "")
    @test Zarr.storagesize(cs, "arr") == Zarr.storagesize(s, "arr")
    @test cs[".zmetadata"] == s.a[".zmetadata"]
  end

  @testset "subkeys delegates to parent" begin
    s = Zarr.DictStore()
    g = zgroup(s)
    zcreate(Int, g, "arr", 4, chunks=(2,))
    cs = Zarr.consolidate_metadata(s, "")
    @test sort(collect(Zarr.subkeys(cs, ""))) == sort(collect(Zarr.subkeys(s, "")))
  end

  @testset "ZarrFormat detection delegates to parent" begin
    s = Zarr.DictStore()
    zgroup(s)
    cs = Zarr.consolidate_metadata(s, "")
    @test Zarr.ZarrFormat(cs, "") == Zarr.ZarrFormat(s, "")
  end

  @testset "_unconcpath" begin
    s = Zarr.DictStore()
    cs = Zarr.ConsolidatedStore(s, "a/b", Dict{String,Any}())
    @test Zarr._unconcpath(cs, "a/b/c/d") == "c/d"
    @test Zarr._unconcpath(cs, "a/b") == ""
    @test_throws ErrorException Zarr._unconcpath(cs, "x/y")
    # with suffix
    @test Zarr._unconcpath(cs, "a/b/c", ".zarray") == "c/.zarray"
  end

  @testset "store_read_strategy and has_configurable_missing_chunks delegate" begin
    s = Zarr.DictStore()
    zgroup(s)
    cs = Zarr.consolidate_metadata(s, "")
    @test Zarr.store_read_strategy(cs) == Zarr.store_read_strategy(s)
    @test Zarr.has_configurable_missing_chunks(cs) == Zarr.has_configurable_missing_chunks(s)
  end

  @testset "v2 full data round-trip through ConsolidatedStore" begin
    s = Zarr.DictStore()
    g = zgroup(s, attrs=Dict("groupatt" => 5))
    a = zcreate(Int, g, "a1", 10, 20, chunks=(5,5), attrs=Dict("arratt" => 2.5))
    a .= reshape(1:200, 10, 20)
    cs = Zarr.consolidate_metadata(s, "")
    g2 = zopen(cs)
    @test g2.attrs == Dict("groupatt" => 5)
    @test g2["a1"].attrs == Dict("arratt" => 2.5)
    @test g2["a1"][:,:] == reshape(1:200, 10, 20)
  end

end