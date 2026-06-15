@testset "Zarr error" begin
  @test_throws ErrorException S3Store("test")
end

using AWSS3

@testset "Path Normalization" begin
    mixed_path = ".\\\\path///to\\a\\place/..\\///"
    norm_path = "path/to/a"
    @test Zarr.normalize_path(mixed_path) == norm_path
    @test Zarr.DirectoryStore(mixed_path).folder == norm_path
    @test Zarr.normalize_path("/") == "/"
    @test Zarr.normalize_path("/a/") == "/a"
    @test Zarr.normalize_path("/path/to/a") == "/path/to/a"
end

@testset "Version and Dimension Separator" begin
  dot_noprefix = Zarr.ChunkKeyEncoding('.', false)
  dot_prefix = Zarr.ChunkKeyEncoding('.', true)
  slash_noprefix = Zarr.ChunkKeyEncoding('/', false)
  slash_prefix = Zarr.ChunkKeyEncoding('/', true)
    let ci = CartesianIndex()
    @test Zarr.citostring(dot_noprefix, ci) == "0"
    @test Zarr.citostring(dot_prefix, ci) == "c.0"
    @test Zarr.citostring(slash_noprefix, ci) == "0"
    @test Zarr.citostring(slash_prefix, ci) == "c/0"
    end
    let ci = CartesianIndex(1,1,1)
    @test Zarr.citostring(dot_noprefix, ci) == "0.0.0"
    @test Zarr.citostring(dot_prefix, ci) == "c.0.0.0"
    @test Zarr.citostring(slash_noprefix, ci) == "0/0/0"
    @test Zarr.citostring(slash_prefix, ci) == "c/0/0/0"
    end
    let ci = CartesianIndex(1,3,5)
    @test Zarr.citostring(dot_noprefix, ci) == "4.2.0"
    @test Zarr.citostring(dot_prefix, ci) == "c.4.2.0"
    @test Zarr.citostring(slash_noprefix, ci) == "4/2/0"
    @test Zarr.citostring(slash_prefix, ci) == "c/4/2/0"
    end
end

"""
Function to test the interface of AbstractStore. Every complete implementation should pass this test.
"""
function test_store_common(ds::Zarr.AbstractStore)
  V = Zarr.DV
  enc = Zarr.ChunkKeyEncoding(Zarr.default_sep(V), Zarr.default_prefix(V))

  @test !Zarr.is_zgroup(V, ds, "")
  ds[".zgroup"]=rand(UInt8,50)
  @test haskey(ds,".zgroup")

  @test Zarr.is_zgroup(V, ds, "")
  @test !Zarr.is_zarray(V, ds, "")

  @test isempty(Zarr.subdirs(ds,""))
  @test sort(collect(Zarr.subkeys(ds,"")))==[".zgroup"]

  #Create a subgroup
  @test !Zarr.is_zarray(V, ds, "bar")
  ds["bar/.zarray"] = rand(UInt8,50)

  @test Zarr.is_zarray(V, ds, "bar")
  @test Zarr.subdirs(ds,"") == ["bar"]
  @test Zarr.subdirs(ds,"bar") == String[]
  #Test getindex and setindex
  data = rand(UInt8,50)

  first_ci_str = Zarr.citostring(enc, CartesianIndex(1, 1, 1))
  second_ci_str = Zarr.citostring(enc, CartesianIndex(2, 1, 1))
  ds["bar/" * first_ci_str] = data
  @test ds["bar/0.0.0"]==data
  @test Zarr.storagesize(ds,"bar")==50
  @test Zarr.isinitialized(ds,"bar/" * first_ci_str)
  @test !Zarr.isinitialized(ds,"bar/" * second_ci_str)
  Zarr.writeattrs(V, ds, "bar", Dict("a" => "b"))
  @test Zarr.getattrs(V, ds, "bar") == Dict("a" => "b")
  delete!(ds,"bar/" * first_ci_str)
  @test !Zarr.store_isinitialized(ds, "bar", CartesianIndex((1, 1, 1)), enc)
  @test !Zarr.isinitialized(ds,"bar/" * first_ci_str)
  ds["bar/" * first_ci_str] = data
  @test !Zarr.store_isinitialized(ds, "bar", CartesianIndex(0, 0, 0), enc)
  @test Zarr.store_isinitialized(ds, "bar", CartesianIndex(1, 1, 1), enc)
  #Add tests for empty storage
  @test Zarr.isemptysub(ds,"ba")
  @test Zarr.isemptysub(ds,"ba/")
  @test !Zarr.isemptysub(ds,"bar")
  @test !Zarr.isemptysub(ds,"bar/")
end

"""
Function to test the interface of a read only AbstractStore. Every complete implementation should pass this test.

`converter` is a function that takes a Zarr.DictStore, and converts it to a read only store.

`closer` is a function that gets called to close the read only store.
"""
function test_read_only_store_common(converter, closer=Returns(nothing))
  V = Zarr.DV
  enc = Zarr.ChunkKeyEncoding(Zarr.default_sep(V), Zarr.default_prefix(V))
  ds = Zarr.DictStore()
  rs = converter(ds)
  @test !Zarr.is_zgroup(V, rs, "")

  closer(rs)
  ds[".zgroup"]=rand(UInt8,50)
  rs = converter(ds)

  @test haskey(rs,".zgroup")

  @test Zarr.is_zgroup(V, rs, "")
  @test !Zarr.is_zarray(V, rs, "")

  @test isempty(Zarr.subdirs(rs,""))
  @test sort(collect(Zarr.subkeys(rs,"")))==[".zgroup"]

  #Create a subgroup
  @test !Zarr.is_zarray(V, rs, "bar")

  closer(rs)
  ds["bar/.zarray"] = rand(UInt8,50)
  rs = converter(ds)

  @test Zarr.is_zarray(V, rs, "bar")
  @test Zarr.subdirs(rs,"") == ["bar"]
  @test Zarr.subdirs(rs,"bar") == String[]
  #Test getindex and setindex
  data = rand(UInt8,50)

  closer(rs)
  ds["bar/0.0.0"] = data
  rs = converter(ds)

  @test rs["bar/0.0.0"]==data
  @test Zarr.storagesize(rs,"bar")==50
  @test Zarr.isinitialized(rs,"bar/0.0.0")
  @test !Zarr.isinitialized(rs,"bar/0.0.1")

  closer(rs)
  Zarr.writeattrs(V, ds, "bar", Dict("a" => "b"))
  rs = converter(ds)

  @test Zarr.getattrs(V, rs, "bar") == Dict("a" => "b")

  closer(rs)
  delete!(ds,"bar/0.0.0")
  rs = converter(ds)

  @test !Zarr.store_isinitialized(rs, "bar", CartesianIndex((0, 0, 0)), enc)
  @test !Zarr.isinitialized(rs,"bar/0.0.0")

  closer(rs)
  ds["bar/0.0.0"] = data
  rs = converter(ds)

  #Add tests for empty storage
  @test Zarr.isemptysub(rs,"ba")
  @test Zarr.isemptysub(rs,"ba/")
  @test !Zarr.isemptysub(rs,"bar")
  @test !Zarr.isemptysub(rs,"bar/")
  closer(rs)
end

@testset "DirectoryStore" begin
  A = fill(1.0, 30, 20)
  chunks = (5,10)
  metadata = Zarr.Metadata(A, chunks; fill_value=-1.5)
  p = tempname()
  mkpath(joinpath(p,"foo"))
  ds = Zarr.DirectoryStore(joinpath(p,"foo"))
  test_store_common(ds)
  @test isdir(joinpath(p,"foo"))
  @test isfile(joinpath(p,"foo",".zgroup"))
  @test isdir(joinpath(p,"foo","bar"))
  @test isfile(joinpath(p,"foo","bar","0.0.0"))
  @test isfile(joinpath(p,"foo","bar",".zarray"))
  #Test that error is thrown when path does not exist and no folder is created
  @test_throws ArgumentError zopen("thisfolderdoesnotexist")
  @test !isdir("thisfolderdoesnotexist")
end

@testset "DictStore" begin
  A = fill(1.0, 30, 20)
  chunks = (5,10)
  metadata = Zarr.Metadata(A, chunks; fill_value=-1.5)
  ds = Zarr.DictStore()
  test_store_common(ds)
  @test haskey(ds.a,".zgroup")
  @test ds.a["bar/.zattrs"]==UInt8[0x7b, 0x22, 0x61, 0x22, 0x3a, 0x22, 0x62, 0x22, 0x7d]
  @test sort(collect(keys(ds.a)))==[".zgroup","bar/.zarray", "bar/.zattrs", "bar/0.0.0"]
end


@testset "Minio S3 storage" begin
  @info "Testing Minio S3 storage"
  A = fill(1.0, 30, 20)
  chunks = (5,10)
  metadata = Zarr.Metadata(A, chunks; fill_value=-1.5)
  using Minio
  if !isnothing(Minio.minio())
    s = Minio.Server(joinpath("./",tempname()), address="localhost:9001")
    run(s, wait=false)
    cfg = MinioConfig("http://localhost:9001")
    ds = AWSS3.AWS.with_aws_config(cfg) do
      AWSS3.S3.create_bucket("zarrdata")
      S3Store("zarrdata")
    end
    test_store_common(ds)
    @test sprint(show, ds) == "S3 Object Storage"
    
    @testset "Pagination" begin
      @info "Testing pagination with Minio S3 storage"
      AWSS3.AWS.with_aws_config(cfg) do
        AWSS3.S3.create_bucket("zarrpagination")
      end
      ds_page = AWSS3.AWS.with_aws_config(cfg) do
        S3Store("zarrpagination")
      end
      for i in 1:1100
        ds_page["dir$(lpad(i,4,'0'))/.zgroup"] = rand(UInt8, 10)
      end
      dirs = Zarr.subdirs(ds_page, "")
      @test length(dirs) == 1100
    end

    kill(s)
  else
    @warn "Skipping Minio Tests, because the package was not built correctly"
  end
end

@testset "AWS S3 Storage" begin
  V = Zarr.DV
  @info "Testing AWS S3 storage"
  S3, p = AWSS3.AWS.with_aws_config(AWSS3.AWS.AWSConfig(creds=nothing, region="us-west-2")) do
    Zarr.storefromstring("s3://mur-sst/zarr-v1")
  end
  @test Zarr.is_zgroup(V, S3, p)
  @test storagesize(S3, p) == 10551
  S3group = zopen(S3,path=p)
  S3Array = S3group["time"]
  @test eltype(S3Array) == Int64
  @test storagesize(S3Array) == 72184
  @test S3Array[1:5] == [0, 1, 2, 3, 4]

  # test with S3Path
  s3_path = S3Path("s3://mur-sst/zarr-v1", config=AWSS3.AWS.AWSConfig(creds=nothing, region="us-west-2"))
  S3group2 = zopen(s3_path)
  S3Array2 = S3group2["time"]
  @test eltype(S3Array2) == Int64
  @test storagesize(S3Array2) == 72184
  @test S3Array2[1:5] == [0, 1, 2, 3, 4]
end

@testset "GCS Storage" begin
  @info "Testing GCS storage"
  for s in (
    "gs://cmip6/CMIP6/HighResMIP/CMCC/CMCC-CM2-HR4/highresSST-present/r1i1p1f1/6hrPlev/psl/gn/v20170706",
    "https://storage.googleapis.com/cmip6/CMIP6/HighResMIP/CMCC/CMCC-CM2-HR4/highresSST-present/r1i1p1f1/6hrPlev/psl/gn/v20170706",
    "http://storage.googleapis.com/cmip6/CMIP6/HighResMIP/CMCC/CMCC-CM2-HR4/highresSST-present/r1i1p1f1/6hrPlev/psl/gn/v20170706",
  )
    cmip6,p = Zarr.storefromstring(s)
    @test cmip6 isa Zarr.GCStore
    @test p == "CMIP6/HighResMIP/CMCC/CMCC-CM2-HR4/highresSST-present/r1i1p1f1/6hrPlev/psl/gn/v20170706"
    @test storagesize(cmip6,p) == 16098
    g = zopen(cmip6,path=p)
    arr = g["psl"]
    @test size(arr) == (288, 192, 97820)
    @test eltype(arr) == Float32
    lat = g["lat"]
    @test size(lat) == (192,)
    @test eltype(lat) == Float64
    @test lat[1:4] == [-90.0,-89.05759162303664,-88.1151832460733,-87.17277486910994]
  end
end

@testset "HTTP Storage" begin
  @info "Testing HTTP Storage"
  s = Zarr.DictStore()
  g = zgroup(s, attrs = Dict("groupatt"=>5))
  a = zcreate(Int,g,"a1",10,20,chunks=(5,5),attrs=Dict("arratt"=>2.5))
  a .= reshape(1:200,10,20)
  using Zarr.HTTP, Sockets
  server = Sockets.listen(0)
  ip,port = getsockname(server)
  @async HTTP.serve(g,ip,port,server=server)
  g2 = zopen("http://$ip:$port")
  @test g2.attrs == Dict("groupatt"=>5)
  @test g2["a1"].attrs == Dict("arratt"=>2.5)
  @test g2["a1"][:,:] == reshape(1:200,10,20)
  
  close(server)

  @testset "HTTPStore construction and show" begin
    hs = Zarr.HTTPStore("http://example.com")
    @test hs.url == "http://example.com"
    @test hs.allowed_codes == Set((404,))
    @test sprint(show, hs) == "HTTP Storage"
    # ConsolidatedStore wrapping an HTTPStore should compose correctly
    cs = Zarr.ConsolidatedStore(hs, "", Dict{String,Any}())
    @test sprint(show, cs) == "Consolidated HTTP Storage"
  end

  @testset "missing_chunk_return_code! on HTTPStore" begin
    hs = Zarr.HTTPStore("http://example.com")
    @test 403 ∉ hs.allowed_codes
    Zarr.missing_chunk_return_code!(hs, 403)
    @test 403 ∈ hs.allowed_codes
    # Vector form
    Zarr.missing_chunk_return_code!(hs, [410, 451])
    @test 410 ∈ hs.allowed_codes
    @test 451 ∈ hs.allowed_codes
  end

  @testset "missing_chunk_return_code! delegates through ConsolidatedStore" begin
    hs = Zarr.HTTPStore("http://example.com")
    # Build a ConsolidatedStore wrapping the HTTPStore directly
    cs = Zarr.ConsolidatedStore(hs, "", Dict{String,Any}())
    Zarr.missing_chunk_return_code!(cs, 403)
    @test 403 ∈ hs.allowed_codes
  end

  @testset "store_read_strategy and has_configurable_missing_chunks" begin
    hs = Zarr.HTTPStore("http://example.com")
    @test Zarr.store_read_strategy(hs) isa Zarr.ConcurrentRead
    @test Zarr.has_configurable_missing_chunks(hs) == true
    # ConsolidatedStore delegates both to parent
    cs = Zarr.ConsolidatedStore(hs, "", Dict{String,Any}())
    @test Zarr.store_read_strategy(cs) isa Zarr.ConcurrentRead
    @test Zarr.has_configurable_missing_chunks(cs) == true
  end

  @testset "storefromstring HTTP/HTTPS regex" begin
    # storefromstring dispatches on the regex list; just check the type comes back right.
    # We use a live local server so the ConsolidatedStore path can succeed.
    s2 = Zarr.DictStore()
    g2 = zgroup(s2)
    server2 = Sockets.listen(0)
    ip2, port2 = getsockname(server2)
    @async HTTP.serve(g2, ip2, port2, server=server2)
    sleep(0.1)
    store, path = Zarr.storefromstring("http://$ip2:$port2")
    @test store isa Zarr.ConsolidatedStore
    @test store.parent isa Zarr.HTTPStore
    @test path == ""
    close(server2)
  end

  @testset "storefromstring falls back gracefully without consolidated metadata" begin
    # A server with no .zmetadata should warn and return a bare HTTPStore
    server3 = Sockets.listen(0)
    ip3, port3 = getsockname(server3)
    # Serve only 404s
    @async HTTP.serve(req -> HTTP.Response(404, "not found"), ip3, port3, server=server3)
    sleep(0.1)
    store, path = @test_warn r"Additional metadata was not available" Zarr.storefromstring("http://$ip3:$port3")
    @test store isa Zarr.HTTPStore
    @test path == ""
    close(server3)
  end

  @testset "zarr_req_handler default 404 path" begin
    s3 = Zarr.DictStore()
    g3 = zgroup(s3, attrs = Dict("x" => 1))
    a3 = zcreate(Int, g3, "b", 4, 4, chunks=(2,2))
    a3 .= reshape(1:16, 4, 4)
    server4 = Sockets.listen(0)
    ip4, port4 = getsockname(server4)
    # zarr_req_handler with default notfound=404
    @async HTTP.serve(Zarr.zarr_req_handler(s3, g3.path), ip4, port4, server=server4)
    sleep(0.1)
    g4 = zopen("http://$ip4:$port4")
    @test g4.attrs == Dict("x" => 1)
    @test g4["b"][:,:] == reshape(1:16, 4, 4)
    # A missing key should return nothing (404 is in the default allowed set)
    hs4 = Zarr.HTTPStore("http://$ip4:$port4")
    @test hs4["nonexistent/chunk"] === nothing
    close(server4)
  end

  @testset "HTTPStore getindex error on unexpected status" begin
    # Server that always returns 500
    server5 = Sockets.listen(0)
    ip5, port5 = getsockname(server5)
    @async HTTP.serve(req -> HTTP.Response(500, "internal error"), ip5, port5, server=server5)
    sleep(0.1)
    hs5 = Zarr.HTTPStore("http://$ip5:$port5")
    @test_throws ErrorException hs5["any/key"]
    close(server5)
  end

  #Test server that returns 403 instead of 404 for missing chunks
  @testset "403 missing chunk workaround" begin
    server6 = Sockets.listen(0)
    ip6, port6 = getsockname(server6)
    s6 = Zarr.DictStore()
    g6 = zgroup(s6, attrs = Dict("groupatt"=>5))
    a6 = zcreate(Int, g6, "a", 10, 20, chunks=(5,5), attrs=Dict("arratt"=>2.5), fill_value=-1)
    @async HTTP.serve(Zarr.zarr_req_handler(s6, g6.path, 403), ip6, port6, server=server6)
    sleep(0.1)
    httpstore6 = Zarr.HTTPStore("http://$ip6:$port6")
    @test_throws "Received error code 403" Zarr.ConsolidatedStore(httpstore6, "")
    Zarr.missing_chunk_return_code!(httpstore6, 403)
    g7 = zopen(Zarr.ConsolidatedStore(httpstore6, ""))
    @test all(==(-1), g7["a"][:,:])
    close(server6)
  end
end

@testset "Zip Storage" begin
  @info "Testing Zip Storage"
  s = Zarr.DictStore()
  g = zgroup(s, attrs = Dict("groupatt"=>5))
  a = zcreate(Int,g,"a1",10,20,chunks=(5,5),attrs=Dict("arratt"=>2.5))
  a .= reshape(1:200,10,20)
  io = IOBuffer()
  Zarr.writezip(io, g)
  data = take!(io)
  ds = Zarr.ZipStore(data)
  @test sprint(show, ds) == "Read Only Zip Storage"
  g2 = zopen(ds)
  @test g2.attrs == Dict("groupatt"=>5)
  @test g2["a1"].attrs == Dict("arratt"=>2.5)
  @test g2["a1"][:,:] == reshape(1:200,10,20)

  test_read_only_store_common() do ds
    # This converts a DictStore to a read only ZipStore
    io = IOBuffer()
    Zarr.writezip(io, ds)
    Zarr.ZipStore(take!(io))
  end
  @info "Finished testing ZipStore"
end

@testset "ConsolidatedStore v3 getattrs" begin
  # canonical: attributes under "attributes" subkey
  store = Zarr.ConsolidatedStore(Zarr.DictStore(), "", Dict{String,Any}(
      "zarr.json" => Dict{String,Any}(
          "node_type" => "group",
          "zarr_format" => 3,
          "attributes" => Dict{String,Any}("foo" => "bar")
      )
  ))
  @test Zarr.getattrs(Zarr.ZarrFormat(3), store, "") == Dict("foo" => "bar")

  # zarr.json present but no "attributes" key: fallback return Dict{String,Any}()
  node_meta = Dict{String,Any}("node_type" => "group", "zarr_format" => 3)
  store_noattrs = Zarr.ConsolidatedStore(Zarr.DictStore(), "", Dict{String,Any}(
      "zarr.json" => node_meta
  ))
  @test Zarr.getattrs(Zarr.ZarrFormat(3), store_noattrs, "") == Dict{String,Any}()

  # missing zarr.json key entirely: empty dict
  store_empty = Zarr.ConsolidatedStore(Zarr.DictStore(), "", Dict{String,Any}())
  @test Zarr.getattrs(Zarr.ZarrFormat(3), store_empty, "") == Dict{String,Any}()
end
@testset "Caching Storage" begin
  # Create source data
  s = Zarr.DictStore()
  g = zgroup(s, attrs = Dict("groupatt"=>5))
  a = zcreate(Int, g, "a1", 10, 20, chunks=(5,5), attrs=Dict("arratt"=>2.5))
  a .= reshape(1:200, 10, 20)

  # Start HTTP server
  using Zarr.HTTP, Sockets
  server = Sockets.listen(0)
  ip, port = getsockname(server)
  @async HTTP.serve(g, ip, port, server=server)
  sleep(0.5)  # wait for server to start

  # Create caching store with temp cache directory
  cache_dir = mktempdir()
  caching_store = Zarr.CachingStore("http://$ip:$port", cache_dir)

  # Wrap in ConsolidatedStore (like HTTPStore requires)
  consolidated = Zarr.ConsolidatedStore(caching_store, "")

  # Open and read data
  g2 = zopen(consolidated)
  @test g2.attrs == Dict("groupatt"=>5)
  @test g2["a1"].attrs == Dict("arratt"=>2.5)
  @test g2["a1"][:,:] == reshape(1:200, 10, 20)

  # Verify data is cached locally
  @test isfile(joinpath(cache_dir, ".zmetadata"))

  # Test show method
  @test sprint(show, caching_store) == "Caching Storage"

  # Stop server
  close(server)

  # The cache directory is itself a (consolidated) zarr store — open it directly.
  g3 = zopen(cache_dir, consolidated=true)
  @test g3.attrs == Dict("groupatt"=>5)
  @test g3["a1"][:,:] == reshape(1:200, 10, 20)

  # Cleanup
  rm(cache_dir, recursive=true)
end
