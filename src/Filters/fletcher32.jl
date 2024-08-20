#=
# Fletcher32 filter

This "filter" basically injects a 4-byte checksum at the end of the data, to ensure data integrity.

The implementation is based on the [numcodecs implementation here](https://github.com/zarr-developers/numcodecs/blob/79d1a8d4f9c89d3513836aba0758e0d2a2a1cfaf/numcodecs/fletcher32.pyx)
and the [original C implementation for NetCDF](https://github.com/Unidata/netcdf-c/blob/main/plugins/H5checksum.c#L109) linked therein.

=#

"""
    Fletcher32Filter()

A compressor that uses the Fletcher32 checksum algorithm to compress and uncompress data.

Note that this goes from UInt8 to UInt8, and is effectively only checking 
the checksum and cropping the last 4 bytes of the data during decoding.
"""
struct Fletcher32Filter <: Filter{UInt8, UInt8}
end

getFilter(::Type{<: Fletcher32Filter}, d::Dict) = Fletcher32Filter()
JSON.lower(::Fletcher32Filter) = Dict("id" => "fletcher32")
filterdict["fletcher32"] = Fletcher32Filter

function _checksum_fletcher32(data::AbstractVector{UInt8})
    len = length(data) / 2 # length in 16-bit words
    sum1::UInt32 = 0
    sum2::UInt32 = 0
    data_idx = 1

    #=
    Compute the checksum for pairs of bytes.
    The magic `360` value is the largest number of sums that can be performed without overflow in UInt32.
    =#
    while len > 0
        tlen = len > 360 ? 360 : len
        len -= tlen
        while tlen > 0
            sum1 += begin # create a 16 bit word from two bytes, the first one shifted to the end of the word
                (UInt16(data[data_idx]) << 8) | UInt16(data[data_idx + 1]) 
            end
            sum2 += sum1
            data_idx += 2
            tlen -= 1
            if tlen < 1
                break
            end
        end
        sum1 = (sum1 & 0xffff) + (sum1 >> 16)
        sum2 = (sum2 & 0xffff) + (sum2 >> 16)
    end

    # if the length of the data is odd, add the first byte to the checksum again (?!)
    if length(data) % 2 == 1 
        sum1 += UInt16(data[1]) << 8
        sum2 += sum1
        sum1 = (sum1 & 0xffff) + (sum1 >> 16)
        sum2 = (sum2 & 0xffff) + (sum2 >> 16)
    end
    return (sum2 << 16) | sum1
end

function zencode(data, ::Fletcher32Filter)
    bytes = reinterpret(UInt8, data)
    checksum = _checksum_fletcher32(bytes)
    result = copy(bytes)
    append!(result, reinterpret(UInt8, [checksum])) # TODO: decompose this without the extra allocation of wrapping in Array
    return result
end

function zdecode(data, ::Fletcher32Filter)
    bytes = reinterpret(UInt8, data)
    checksum = _checksum_fletcher32(view(bytes, 1:length(bytes) - 4))
    stored_checksum = only(reinterpret(UInt32, view(bytes, (length(bytes) - 3):length(bytes))))
    if checksum != stored_checksum
        throw(ErrorException("""
        Checksum mismatch in Fletcher32 decoding.  
        
        The computed value is $(checksum) and the stored value is $(stored_checksum).  
        This might be a sign that the data is corrupted.
        """)) # TODO: make this a custom error type
    end
    return view(bytes, 1:length(bytes) - 4)
end
