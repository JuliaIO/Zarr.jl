@testset "ArrayCache" begin
    s = Zarr.DictStore()
    g = zgroup(s, attrs = Dict("groupatt"=>5))
    a = zcreate(Int, g, "a1", 10, 20, chunks=(5,5), attrs=Dict("arratt"=>2.5))
    a .= reshape(1:200, 10, 20)

    # Start HTTP server
    using Zarr.HTTP, Sockets
    server = Sockets.listen(0)
    ip, port = getsockname(server)
    @async HTTP.serve(g, ip, port,server=server)
    sleep(0.5)  # wait for server to start

    # Create caching store with temp cache directory
    cache_dir = tempname()
    base_array = zopen("http://$ip:$port")


    g2 = zarrcache(base_array, cache_dir)
    @test g2["a1"].cache.a.storage.folder == joinpath(cache_dir,"a1")
    # We also open the cache array on disk directly
    g_disk = zopen(cache_dir)
    @test g_disk.attrs == Dict("groupatt"=>5)
    @test g_disk["a1"].attrs == Dict("arratt"=>2.5)
    @test_throws ArgumentError g_disk["a1"][1,1]
    # Now we access some data 
    @test g2["a1"][1:5,1:5] == a[1:5,1:5]
    #and the data should be cached
    @test g_disk["a1"][1:5,1:5] == a[1:5,1:5]
    # While others still are not
    @test_throws ArgumentError g_disk["a1"][6,1]

    # Now test if we can open the cache store from an existing path
    g3 = zarrcache(base_array, cache_dir)
    @test g3["a1"].cache.a.storage.folder == cache_dir * "/a1"
    @test g3["a1"][1:5,6:10] == a[1:5,6:10]
    @test g_disk["a1"][1:5,6:10] == a[1:5,6:10]
    # Stop server
    close(server)
    # Cleanup
    rm(cache_dir, recursive=true)
end