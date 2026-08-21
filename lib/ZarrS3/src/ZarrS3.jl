"""
    ZarrS3

S3 storage type and URL registration. AWSS3 methods load through the
`ZarrS3AWSS3Ext` extension.
"""
module ZarrS3

# The extension qualifies methods that extend ZarrCore generics.
import ZarrCore
using ZarrCore: AbstractStore, storageregexlist

"""
    S3Store(bucket::String; aws=nothing)

An S3-backed Zarr store. Available after loading the `ZarrS3AWSS3Ext`
extension, i.e. after `using AWSS3`.
"""
struct S3Store <: AbstractStore
    bucket::String
    aws::Any
end

# The extension adds the more-specific keyword constructor.
function S3Store(args...)
    error("AWSS3 must be loaded to use S3Store. Try `using AWSS3`.")
end

# Register without AWSS3 so `s3://` URLs resolve to the fallback constructor.
function __init__()
    push!(storageregexlist, r"^s3://" => S3Store)
end

export S3Store

@static if VERSION >= v"1.11"
    include("public_names_s3.jl")
end

end # module
