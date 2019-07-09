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
  @test !Zarr.is_zgroup(ds)
  ds[".zgroup"]=rand(UInt8,50)
  @test Zarr.is_zgroup(ds)
  @test !Zarr.is_zarray(ds)

  @test Zarr.zname(ds)=="foo"
  @test isempty(Zarr.subdirs(ds))
  @test sort(collect(Zarr.keys(ds)))==[".zgroup"]

  #Create a subgroup
  snew = Zarr.newsub(ds,"bar")
  @test !Zarr.is_zarray(ds)
  snew[".zarray"] = rand(UInt8,50)
  @test Zarr.is_zgroup(ds)
  #Test getindex and setindex
  data = rand(UInt8,50)
  snew["0.0.0"] = data
  @test snew["0.0.0"]==data
  @test Zarr.storagesize(snew)==50
  @test Zarr.isinitialized(snew,"0.0.0")
  @test !Zarr.isinitialized(snew,"0.0.1")
  Zarr.writeattrs(snew,Dict("a"=>"b"))
  @test Zarr.getattrs(snew)==Dict("a"=>"b")
  snew2 = Zarr.getsub(ds,"bar")
  @test Zarr.getattrs(snew2)==Dict("a"=>"b")
  @test snew2["0.0.0"]==data
  delete!(snew2,"0.0.0")
  @test !Zarr.isinitialized(snew2,"0.0.0")
  snew["0.0.0"] = data
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
  @test Zarr.path(ds)==joinpath(p,"foo")
end

@testset "DictStore" begin
  A = fill(1.0, 30, 20)
  chunks = (5,10)
  metadata = Zarr.Metadata(A, chunks; fill_value=-1.5)
  ds = Zarr.DictStore("foo")
  test_store_common(ds)
  @test ds.name == "foo"
  @test haskey(ds.a,".zgroup")
  @test ds.subdirs["bar"].a[".zattrs"]==UInt8[0x7b, 0x22, 0x61, 0x22, 0x3a, 0x22, 0x62, 0x22, 0x7d]
  @test sort(collect(keys(ds.subdirs["bar"].a)))==[".zarray", ".zattrs", "0.0.0"]
  @test isempty(keys(ds.subdirs["bar"].subdirs))
end

# @testset "AWS S3 Storage" begin
#     bucket = "zarr-demo"
#     store = "store/foo/"
#     region = "eu-west-2"
#     S3 = S3Store(bucket, store, region)
#     @test storagesize(S3) == 69
#     @test Zarr.zname(S3) == "foo"
#     @test Zarr.is_zgroup(S3) == true
#     S3group = zopen(S3)
#     @test Zarr.zname(S3group) == "foo"
#     S3Array = S3group.groups["bar"].arrays["baz"]
#     @test Zarr.zname(S3Array) == "baz"
#     @test eltype(S3Array) == Zarr.ASCIIChar
#     @test storagesize(S3Array) == 69
#     @test String(S3Array[:]) == "Hello from the cloud!"
# end
