@testset "subpackage registration" begin
    compressors = ZarrCore.compressortypes
    codec_parsers = Zarr.Codecs.V3Codecs.codec_parsers
    storage_entries = ZarrCore.storageregexlist.entries

    compressor_snapshot = copy(compressors)
    codec_snapshot = copy(codec_parsers)
    storage_snapshot = copy(storage_entries)

    compressor_types = Dict(
        "blosc" => Zarr.BloscCompressor,
        "zlib" => Zarr.ZlibCompressor,
        "zstd" => Zarr.ZstdCompressor,
    )
    codec_types = Dict(
        "blosc" => Zarr.BloscV3Codec,
        "gzip" => Zarr.GzipV3Codec,
        "zstd" => Zarr.ZstdV3Codec,
    )
    url_types = Dict(
        "^https://storage.googleapis.com" => Zarr.GCStore,
        "^http://storage.googleapis.com" => Zarr.GCStore,
        "^gs://" => Zarr.GCStore,
        "^https://" => Zarr.HTTPStore,
        "^http://" => Zarr.HTTPStore,
        "^s3://" => Zarr.S3Store,
    )

    try
        @test all(get(compressors, name, nothing) === T
                  for (name, T) in compressor_types)
        @test all(haskey(codec_parsers, name) && codec_parsers[name].return_type === T
                  for (name, T) in codec_types)
        @test all(any(p -> first(p).pattern == pattern && last(p) === T,
                      storage_entries) for (pattern, T) in url_types)

        foreach(name -> delete!(compressors, name), keys(compressor_types))
        foreach(name -> delete!(codec_parsers, name), keys(codec_types))
        filter!(p -> !haskey(url_types, first(p).pattern), storage_entries)

        Zarr.ZarrBlosc.register!()
        Zarr.ZarrZlib.register!()
        Zarr.ZarrZstd.register!()
        Zarr.ZarrHTTP.register!()
        Zarr.ZarrGCS.register!()
        Zarr.ZarrS3.register!()

        @test all(compressors[name] === T for (name, T) in compressor_types)
        @test all(codec_parsers[name].return_type === T for (name, T) in codec_types)
        @test all(any(p -> first(p).pattern == pattern && last(p) === T,
                      storage_entries) for (pattern, T) in url_types)

        gcs_index = findfirst(p -> first(p).pattern == "^https://storage.googleapis.com",
                              storage_entries)
        http_index = findfirst(p -> first(p).pattern == "^https://", storage_entries)
        @test gcs_index < http_index

        before_zip = (copy(compressors), copy(codec_parsers), copy(storage_entries))
        @test Zarr.ZarrZip.register!() === nothing
        @test before_zip == (compressors, codec_parsers, storage_entries)

        values = reshape(Int32.(1:12), 3, 4)
        cases = (
            (Zarr.BloscCompressor(), "blosc", "blosc"),
            (Zarr.ZlibCompressor(), "zlib", "gzip"),
            (Zarr.ZstdCompressor(), "zstd", "zstd"),
        )
        for (compressor, v2_name, v3_name) in cases
            v2_store = Zarr.DictStore()
            v2 = zcreate(Int32, v2_store, 3, 4; chunks=(3, 4), compressor)
            v2[:, :] = values
            v2_metadata = JSON.parse(String(copy(v2_store[".zarray"])))
            @test v2_metadata["compressor"]["id"] == v2_name
            @test zopen(v2_store)[:, :] == values

            v3_store = Zarr.DictStore()
            v3 = zcreate(Int32, v3_store, 3, 4; zarr_format=3,
                         chunks=(3, 4), compressor, fill_value=Int32(0))
            v3[:, :] = values
            v3_metadata = JSON.parse(String(copy(v3_store["zarr.json"])))
            @test any(codec -> codec["name"] == v3_name, v3_metadata["codecs"])
            @test zopen(v3_store)[:, :] == values
        end
    finally
        empty!(compressors)
        merge!(compressors, compressor_snapshot)
        empty!(codec_parsers)
        merge!(codec_parsers, codec_snapshot)
        empty!(storage_entries)
        append!(storage_entries, storage_snapshot)
    end
end
