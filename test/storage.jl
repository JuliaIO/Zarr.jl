@testset "Path Normalization" begin
    mixed_path = ".\\\\path///to\\a\\place/..\\///"
    norm_path = "path/to/a"
    @test ZarrNative.normalize_path(mixed_path) == norm_path
    @test ZarrNative.DirectoryStore(mixed_path).folder == norm_path
    @test ZarrNative.normalize_path("/") == "/"
    @test ZarrNative.normalize_path("/a/") == "/a"
    @test ZarrNative.normalize_path("/path/to/a") == "/path/to/a"
end

"""
Function to test the interface of AbstractStore. Every complete implementation should pass this test.
"""
function test_store_common(ds)
  @test !ZarrNative.is_zgroup(ds)
  ds[".zgroup"]=rand(UInt8,50)
  @test ZarrNative.is_zgroup(ds)
  @test !ZarrNative.is_zarray(ds)

  @test ZarrNative.zname(ds)=="foo"
  @test isempty(ZarrNative.subdirs(ds))
  @test sort(collect(ZarrNative.keys(ds)))==[".zgroup"]

  #Create a subgroup
  snew = ZarrNative.newsub(ds,"bar")
  @test !ZarrNative.is_zarray(ds)
  snew[".zarray"] = rand(UInt8,50)
  @test ZarrNative.is_zgroup(ds)
  #Test getindex and setindex
  data = rand(UInt8,50)
  snew["0.0.0"] = data
  @test snew["0.0.0"]==data
  @test ZarrNative.storagesize(snew)==50
  @test ZarrNative.isinitialized(snew,"0.0.0")
  @test !ZarrNative.isinitialized(snew,"0.0.1")
  ZarrNative.writeattrs(snew,Dict("a"=>"b"))
  @test ZarrNative.getattrs(snew)==Dict("a"=>"b")
  snew2 = ZarrNative.getsub(ds,"bar")
  @test ZarrNative.getattrs(snew2)==Dict("a"=>"b")
  @test snew2["0.0.0"]==data
end

@testset "DirectoryStore" begin
  A = fill(1.0, 30, 20)
  chunks = (5,10)
  metadata = ZarrNative.Metadata(A, chunks; fill_value=-1.5)
  p = tempname()
  mkpath(joinpath(p,"foo"))
  ds = ZarrNative.DirectoryStore(joinpath(p,"foo"))
  test_store_common(ds)
  @test isdir(joinpath(p,"foo"))
  @test isfile(joinpath(p,"foo",".zgroup"))
  @test isdir(joinpath(p,"foo","bar"))
  @test isfile(joinpath(p,"foo","bar","0.0.0"))
  @test isfile(joinpath(p,"foo","bar",".zarray"))
  @test ZarrNative.path(ds)==joinpath(p,"foo")
end

@testset "DictStore" begin
  A = fill(1.0, 30, 20)
  chunks = (5,10)
  metadata = ZarrNative.Metadata(A, chunks; fill_value=-1.5)
  ds = ZarrNative.DictStore("foo")
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
#     @test ZarrNative.zname(S3) == "foo"
#     @test ZarrNative.is_zgroup(S3) == true
#     S3group = zopen(S3)
#     @test ZarrNative.zname(S3group) == "foo"
#     S3Array = S3group.groups["bar"].arrays["baz"]
#     @test ZarrNative.zname(S3Array) == "baz"
#     @test eltype(S3Array) == ZarrNative.ASCIIChar
#     @test storagesize(S3Array) == 69
#     @test String(S3Array[:]) == "Hello from the cloud!"
# end
