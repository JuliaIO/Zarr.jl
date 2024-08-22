using Test

using Zarr: zencode, zdecode
using Zarr: Fletcher32Filter

@testset "Fletcher32Filter" begin
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
    @test reinterpret(Int64, zdecode(bit_data, Fletcher32Filter())) == expected
    @test zencode(expected, Fletcher32Filter()) == bit_data

    for Typ in (UInt8, Int32, Float32, Float64)
        arr = rand(Typ, 100)
        @test reinterpret(Typ, zdecode(zencode(arr, Fletcher32Filter()), Fletcher32Filter())) == arr
    end

    data = rand(100)
    enc = zencode(data, Fletcher32Filter())
    enc[begin] += 1
    @test_throws "Checksum mismatch in Fletcher32 decoding" zdecode(enc, Fletcher32Filter())
end

#=
@testset "FixedScaleOffsetFilter" begin
    arrays = [
        LinRange{Float64}(1000, 1001, 1000),
        randn(1000) .+ 1000,
        reshape(LinRange{Float64}(1000, 1001, 1000), (100, 10)),
        reshape(LinRange{Float64}(1000, 1001, 1000), (10, 10, 10)),
    ]

    codecs = [
        FixedScaleOffsetFilter(offset = 1000, scale = 1, T = Float64, Tenc = Int8),
        FixedScaleOffsetFilter(offset = 1000, scale = 10^2, T = Float64, Tenc = Int16),
        FixedScaleOffsetFilter(offset = 1000, scale = 10^6, T = Float64, Tenc = Int32),
        FixedScaleOffsetFilter(offset = 1000, scale = 10^12, T = Float64, Tenc = Int64),
        FixedScaleOffsetFilter(offset = 1000, scale = 10^12, T = Float64),
    ]

    for array in arrays
        for codec in codecs
            encoded = Zarr.zencode(array, codec)
            decoded = Zarr.zdecode(encoded, codec)
            decimal = round(log10(codec.scale))
            @test decoded ≈ array rtol=1.5*10^(-decimal)
        end
    end
end

=#