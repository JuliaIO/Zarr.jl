using Test

using Zarr: zcompress, zuncompress
using Zarr: Fletcher32Compressor

@testset "Fletcher32Compressor" begin
    # These tests are copied exactly from the [`numcodecs`](https://github.com/zarr-developers/numcodecs/) Python package,
    # specifically [this file](https://github.com/zarr-developers/numcodecs/blob/main/numcodecs/tests/test_fletcher32.py).
    
    bit_data = vcat(
        b"w\x07\x00\x00\x00\x00\x00\x00\x85\xf6\xff\xff\xff\xff\xff\xff",
        b"i\x07\x00\x00\x00\x00\x00\x00\x94\xf6\xff\xff\xff\xff\xff\xff",
        b"\x88\t\x00\x00\x00\x00\x00\x00i\x03\x00\x00\x00\x00\x00\x00",
        b"\x93\xfd\xff\xff\xff\xff\xff\xff\xc3\xfc\xff\xff\xff\xff\xff\xff",
        b"'\x02\x00\x00\x00\x00\x00\x00\xba\xf7\xff\xff\xff\xff\xff\xff",
        b"\xfd%\x86d",
    )
    expected = [1911, -2427, 1897, -2412, 2440, 873, -621, -829, 551, -2118]
    @test zuncompress(bit_data, Fletcher32Compressor(), Int64) == expected
    @test zcompress(expected, Fletcher32Compressor()) == bit_data

    for Typ in (UInt8, Int32, Float32, Float64)
        arr = rand(Typ, 100)
        @test zuncompress(zcompress(arr, Fletcher32Compressor()), Fletcher32Compressor(), Typ) == arr
    end

    data = rand(100)
    enc = zcompress(data, Fletcher32Compressor())
    enc[begin] += 1
    @test_throws "Checksum mismatch in Fletcher32 compression" zuncompress(enc, Fletcher32Compressor(), Int64)
end