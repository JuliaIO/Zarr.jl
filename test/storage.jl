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
    v2cke_period = Zarr.V2ChunkKeyEncoding{'.'}
    v2cke_slash = Zarr.V2ChunkKeyEncoding{'/'}
    let ci = CartesianIndex()
        @test Zarr.citostring(ci, 2, '.') == "0"
        @test Zarr.citostring(ci, 2, '/') == "0"
        @test Zarr.citostring(ci, 3, v2cke_period) == "0"
        @test Zarr.citostring(ci, 3, v2cke_slash) == "0"
        @test Zarr.citostring(ci, 3, '.') == "c.0"
        @test Zarr.citostring(ci, 3, '/') == "c/0"
    end
    let ci = CartesianIndex(1,1,1)
        @test Zarr.citostring(ci, 2, '.') == "0.0.0"
        @test Zarr.citostring(ci, 2, '/') == "0/0/0"
        @test Zarr.citostring(ci, 3, v2cke_period) == "0.0.0"
        @test Zarr.citostring(ci, 3, v2cke_slash) == "0/0/0"
        @test Zarr.citostring(ci, 3, '.') == "c.0.0.0"
        @test Zarr.citostring(ci, 3, '/') == "c/0/0/0"
    end
    let ci = CartesianIndex(1,3,5)
        @test Zarr.citostring(ci, 2, '.') == "4.2.0"
        @test Zarr.citostring(ci, 2, '/') == "4/2/0"
        @test Zarr.citostring(ci, 3, v2cke_period) == "4.2.0"
        @test Zarr.citostring(ci, 3, v2cke_slash) == "4/2/0"
        @test Zarr.citostring(ci, 3, '.') == "c.4.2.0"
        @test Zarr.citostring(ci, 3, '/') == "c/4/2/0"
    end
end

"""
Function to test the interface of AbstractStore. Every complete implementation should pass this test.
"""
function test_store_common(ds::Zarr.AbstractStore)
  @test !Zarr.is_zgroup(ds,"")
  ds[".zgroup"]=rand(UInt8,50)
  @test haskey(ds,".zgroup")

  @test Zarr.is_zgroup(ds,"")
  @test !Zarr.is_zarray(ds,"")

  @test isempty(Zarr.subdirs(ds,""))
  @test sort(collect(Zarr.subkeys(ds,"")))==[".zgroup"]

  #Create a subgroup
  @test !Zarr.is_zarray(ds,"bar")
  ds["bar/.zarray"] = rand(UInt8,50)

  @test Zarr.is_zarray(ds,"bar")
  @test Zarr.subdirs(ds,"") == ["bar"]
  @test Zarr.subdirs(ds,"bar") == String[]
  #Test getindex and setindex
  data = rand(UInt8,50)
  V = Zarr.zarr_format(ds)
  S = Zarr.dimension_separator(ds)
  first_ci_str = Zarr.citostring(CartesianIndex(1,1,1), V, S)
  second_ci_str = Zarr.citostring(CartesianIndex(2,1,1), V, S)
  ds["bar/" * first_ci_str] = data
  @test ds["bar/0.0.0"]==data
  @test Zarr.storagesize(ds,"bar")==50
  @test Zarr.isinitialized(ds,"bar/" * first_ci_str)
  @test !Zarr.isinitialized(ds,"bar/" * second_ci_str)
  Zarr.writeattrs(ds,"bar",Dict("a"=>"b"))
  @test Zarr.getattrs(ds,"bar")==Dict("a"=>"b")
  delete!(ds,"bar/" * first_ci_str)
  @test !Zarr.isinitialized(ds,"bar",CartesianIndex((1,1,1)))
  @test !Zarr.isinitialized(ds,"bar/" * first_ci_str)
  ds["bar/" * first_ci_str] = data
  @test !Zarr.isinitialized(ds, "bar", CartesianIndex(0,0,0))
  @test Zarr.isinitialized(ds, "bar", CartesianIndex(1,1,1))
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
  ds = Zarr.DictStore()
  rs = converter(ds)
  @test !Zarr.is_zgroup(rs,"")

  closer(rs)
  ds[".zgroup"]=rand(UInt8,50)
  rs = converter(ds)

  @test haskey(rs,".zgroup")

  @test Zarr.is_zgroup(rs,"")
  @test !Zarr.is_zarray(rs,"")

  @test isempty(Zarr.subdirs(rs,""))
  @test sort(collect(Zarr.subkeys(rs,"")))==[".zgroup"]

  #Create a subgroup
  @test !Zarr.is_zarray(rs,"bar")

  closer(rs)
  ds["bar/.zarray"] = rand(UInt8,50)
  rs = converter(ds)

  @test Zarr.is_zarray(rs,"bar")
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
  Zarr.writeattrs(ds,"bar",Dict("a"=>"b"))
  rs = converter(ds)

  @test Zarr.getattrs(rs,"bar")==Dict("a"=>"b")

  closer(rs)
  delete!(ds,"bar/0.0.0")
  rs = converter(ds)

  @test !Zarr.isinitialized(rs,"bar",CartesianIndex((0,0,0)))
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
  A = fill(1.0, 30, 20)
  chunks = (5,10)
  metadata = Zarr.Metadata(A, chunks; fill_value=-1.5)
  using Minio
  if !isnothing(Minio.minio())
    s = Minio.Server(joinpath("./",tempname()), address="localhost:9001")
    run(s, wait=false)
    cfg = MinioConfig("http://localhost:9001")
    Zarr.AWSS3.global_aws_config(cfg)
    Zarr.AWSS3.S3.create_bucket("zarrdata")
    ds = S3Store("zarrdata")
    test_store_common(ds)
    @test sprint(show, ds) == "S3 Object Storage"
    kill(s)
  else
    @warn "Skipping Minio Tests, because the package was not built correctly"
  end
end

@testset "AWS S3 Storage" begin
  Zarr.AWSS3.AWS.global_aws_config(Zarr.AWSS3.AWS.AWSConfig(creds=nothing, region="us-west-2"))
  S3, p = Zarr.storefromstring("s3://mur-sst/zarr-v1")
  @test Zarr.is_zgroup(S3, p)
  @test storagesize(S3, p) == 10551
  S3group = zopen(S3,path=p)
  S3Array = S3group["time"]
  @test eltype(S3Array) == Int64
  @test storagesize(S3Array) == 72184
  @test S3Array[1:5] == [0, 1, 2, 3, 4]
end

@testset "GCS Storage" begin
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
  
  # The following test doesn't pass, but maybe should?
  # test_read_only_store_common() do ds
  #   # This converts a DictStore to a read only ConsolidatedStore HTTPStore
  #   @async HTTP.serve(ds,"",ip,port,server=server)
  #   Zarr.ConsolidatedStore(Zarr.HTTPStore("http://$ip:$port"),"")
  # end
  close(server)
  #Test server that returns 403 instead of 404 for missing chunks
  server = Sockets.listen(0)
  ip,port = getsockname(server)
  s = Zarr.DictStore()
  g = zgroup(s, attrs = Dict("groupatt"=>5))
  a = zcreate(Int,g,"a",10,20,chunks=(5,5),attrs=Dict("arratt"=>2.5),fill_value = -1)
  @async HTTP.serve(Zarr.zarr_req_handler(s,g.path,403),ip,port,server=server)
  g3 = zopen("http://$ip:$port")
  @test_throws "Received error code 403" g3["a"][:,:]
  Zarr.missing_chunk_return_code!(g3.storage,403)
  @test all(==(-1),g3["a"][:,:])
  close(server)
end

@testset "Zip Storage" begin
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
end
