@testset "Path Normalization" begin
    mixed_path = ".\\\\path///to\\a\\place/..\\///"
    norm_path = "path/to/a"
    @test Zarr.normalize_path(mixed_path) == norm_path
    @test Zarr.DirectoryStore(mixed_path).folder == norm_path
    @test Zarr.normalize_path("/") == "/"
    @test Zarr.normalize_path("/a/") == "/a"
    @test Zarr.normalize_path("/path/to/a") == "/path/to/a"
end

"""
Function to test the interface of AbstractStore. Every complete implementation should pass this test.
"""
function test_store_common(ds)
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
  ds["bar/0.0.0"] = data
  @test ds["bar/0.0.0"]==data
  @test Zarr.storagesize(ds,"bar")==50
  @test Zarr.isinitialized(ds,"bar/0.0.0")
  @test !Zarr.isinitialized(ds,"bar/0.0.1")
  Zarr.writeattrs(ds,"bar",Dict("a"=>"b"))
  @test Zarr.getattrs(ds,"bar")==Dict("a"=>"b")
  delete!(ds,"bar/0.0.0")
  @test !Zarr.isinitialized(ds,"bar",CartesianIndex((0,0,0)))
  @test !Zarr.isinitialized(ds,"bar/0.0.0")
  ds["bar/0.0.0"] = data
  #Add tests for empty storage
  @test Zarr.isemptysub(ds,"ba")
  @test Zarr.isemptysub(ds,"ba/")
  @test !Zarr.isemptysub(ds,"bar")
  @test !Zarr.isemptysub(ds,"bar/")
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

@testset "LRU" begin
  p = tempname()
  g = zgroup(p)
  a = zcreate(Float64,g,"foo",  1000,1000, chunks=(1000,10), fill_value=NaN)
  a[1:500000] = 1:500000
  g2 = zopen(p,"w",lru = 5)
  newstore = g2.storage
  @test newstore isa Zarr.LRUStore
  a2 = g2["foo"]
  @test isempty(keys(newstore.lru))
  #Read some data to fill cache
  @test a2[1:10,1] == 1:10
  @test haskey(newstore.lru,"foo/0.0")
  @test newstore.lru["foo/0.0"] == reshape(1:10000,1000,10)
  # Now we modify the lru store to test that data is actually read from there
  newstore.lru["foo/0.0"] .= 0
  @test all(iszero,a2[1:10,1])
  @test_throws ErrorException a2[1,1] = 5
  @test_throws ErrorException delete!(newstore,"foo/0.0")
  @test isnan(a2[1,1000])
  @test storagesize(a2) == 83002
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
  Zarr.AWSS3.AWS.global_aws_config(Zarr.AWSS3.AWS.AWSConfig(creds=nothing, region="eu-west-2"))
  S3,p = Zarr.storefromstring("s3://zarr-demo/store/foo")
  @test storagesize(S3,p) == 0
  @test Zarr.is_zgroup(S3,p) == true
  S3group = zopen(S3,path=p)
  S3Array = S3group.groups["bar"].arrays["baz"]
  @test eltype(S3Array) == Zarr.ASCIIChar
  @test storagesize(S3Array) == 69
  @test String(S3Array[:]) == "Hello from the cloud!"
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
  close(server)
end
