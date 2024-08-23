using Test
using Zarr: DateTime64 # for datetime reinterpret

using Zarr: zencode, zdecode
using Zarr: Fletcher32Filter, FixedScaleOffsetFilter, ShuffleFilter, QuantizeFilter

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
@testset "ShuffleFilter" begin

    codecs = [
        ShuffleFilter(),
        ShuffleFilter(elementsize=0),
        ShuffleFilter(elementsize=4),
        ShuffleFilter(elementsize=8),
    ]

    arrays = [
        Int32.(collect(1:1000)),                                                        # equivalent to np.arange(1000, dtype='i4')
        LinRange(1000, 1001, 1000),                                                     # equivalent to np.linspace(1000, 1001, 1000, dtype='f8')
        reshape(randn(1000) .* 1 .+ 1000, (100, 10)),                                   # equivalent to np.random.normal(loc=1000, scale=1, size=(100, 10))
        reshape(rand(Bool, 1000), (10, 100)),                                           # equivalent to np.random.randint(0, 2, size=1000, dtype=bool).reshape(100, 10, order='F')
        reshape(rand(Zarr.MaxLengthString{3, UInt8}["a", "bb", "ccc"], 1000), (10, 10, 10)), # equivalent to np.random.choice([b'a', b'bb', b'ccc'], size=1000).reshape(10, 10, 10)
        reinterpret(DateTime64{Dates.Nanosecond}, rand(UInt64(0):UInt64(2^60)-1, 1000)), # equivalent to np.random.randint(0, 2**60, size=1000, dtype='u8').view('M8[ns]')
        Nanosecond.(rand(UInt64(0):UInt64(2^60-1), 1000)),                              # equivalent to np.random.randint(0, 2**60, size=1000, dtype='u8').view('m8[ns]')
        reinterpret(DateTime64{Dates.Minute}, rand(UInt64(0):UInt64(2^25-1), 1000)),    # equivalent to np.random.randint(0, 2**25, size=1000, dtype='u8').view('M8[m]')
        Minute.(rand(UInt64(0):UInt64(2^25-1), 1000)),                                  # equivalent to np.random.randint(0, 2**25, size=1000, dtype='u8').view('m8[m]')
        reinterpret(DateTime64{Dates.Nanosecond}, rand(Int64(-(2^63)):Int64(-(2^63)+20), 1000)),    # equivalent to np.random.randint(-(2**63), -(2**63) + 20, size=1000, dtype='i8').view('M8[ns]')
        Nanosecond.(rand(Int64(-(2^63)):Int64(-(2^63)+20), 1000)),                      # equivalent to np.random.randint(-(2**63), -(2**63) + 20, size=1000, dtype='i8').view('m8[ns]')
        reinterpret(DateTime64{Dates.Minute}, rand(Int64(-(2^63)):Int64(-(2^63)+20), 1000)),    # equivalent to np.random.randint(-(2**63), -(2**63) + 20, size=1000, dtype='i8').view('M8[m]')
        Minute.(rand(Int64(-(2^63)):Int64(-(2^63)+20), 1000)),                          # equivalent to np.random.randint(-(2**63), -(2**63) + 20, size=1000, dtype='i8').view('m8[m]')
    ]

    for codec in codecs
        for array in arrays
            encoded = Zarr.zencode(array, codec)
            decoded = reshape(reinterpret(eltype(array), Zarr.zdecode(encoded, codec)), size(array))
            @test decoded == array
        end
    end
end


@testset "QuantizeFilter" begin

    codecs = [
        QuantizeFilter{Float64, Float16}(digits=-1),
        QuantizeFilter{Float64, Float16}(digits=0),
        QuantizeFilter{Float64, Float16}(digits=1),
        QuantizeFilter{Float64, Float32}(digits=5),
        QuantizeFilter{Float64, Float64}(digits=12),
    ]

    arrays = [
        LinRange(100, 200, 1000),                         # np.linspace(100, 200, 1000, dtype='<f8')
        randn(1000) .+ 0,                                 # np.random.normal(loc=0, scale=1, size=1000).astype('<f8')
        reshape(LinRange(100, 200, 1000), (100, 10)),     # np.linspace(100, 200, 1000, dtype='<f8').reshape(100, 10)
        permutedims(reshape(LinRange(100, 200, 1000), (10, 100))),  # np.linspace(100, 200, 1000, dtype='<f8').reshape(100, 10, order='F')
        reshape(LinRange(100, 200, 1000), (10, 10, 10)),  # np.linspace(100, 200, 1000, dtype='<f8').reshape(10, 10, 10)
    ]

    @testset "Encoding accuracy" begin
        for codec in codecs
            @testset "$(codec.digits) digits" begin
                for array in arrays
                    encoded = Zarr.zencode(array, codec)
                    decoded = reshape(reinterpret(eltype(array), Zarr.zdecode(encoded, codec)), size(array))
                    @test decoded ≈ array rtol=(1.5*10.0^(-codec.digits))
                end
            end
        end
    end

    @testset "Decode is a no-op" begin
        for codec in codecs
            @testset "$(codec.digits) digits" begin
                for array in arrays
                    encoded = Zarr.zencode(array, codec)
                    decoded = Zarr.zdecode(encoded, codec)
                    @test decoded === encoded
                end
            end
        end
    end
end