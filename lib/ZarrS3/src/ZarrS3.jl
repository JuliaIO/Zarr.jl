"""
    ZarrS3

Amazon S3 (and S3-compatible object storage) support for Zarr.jl: the
[`S3Store`](@ref) type and the `s3://` URL registration.

Unlike the other backend subpackages, the actual implementation lives in a
weak-dependency extension (`ZarrS3AWSS3Ext`) and only loads once AWSS3.jl is:
AWSS3 costs roughly a third of a second to load, which is more than the whole
rest of Zarr, and most sessions never touch S3. What this package holds instead
is everything that has to exist *without* AWSS3:

- the `S3Store` type, so `Zarr.S3Store` always resolves and stores can be
  referred to (in signatures, in `zopen` dispatch) before AWSS3 is loaded;
- the fallback constructor, so `S3Store("bucket")` fails with a friendly
  "load AWSS3" message rather than a `MethodError`;
- the `^s3://` entry in `ZarrCore.storageregexlist`, so `zopen("s3://...")`
  reaches that same message instead of "no storage type matched".

`Zarr` hard-depends on this package, so the behaviour a user sees is exactly
what it was when `S3Store` lived in `ZarrCore` and the extension hung off the
`Zarr` umbrella.
"""
module ZarrS3

# Only the names used unqualified live here. Methods that *extend* a ZarrCore
# generic are always written as `ZarrCore.f(...)` (see the extension): a bare
# `f(...)` definition would silently create a new function that shadows the
# generic instead of adding a method to it.
import ZarrCore
using ZarrCore: @public, AbstractStore, storageregexlist

"""
    PUBLIC_NAMES::Vector{Symbol}

Every name this module declares with `ZarrCore.@public`, in declaration order.
See `ZarrCore.PUBLIC_NAMES` -- this registry is per-module and is what lets the
`Zarr` facade re-export the public API on Julia 1.10, which has no `public`.
"""
const PUBLIC_NAMES = Symbol[]

"""
    S3Store(bucket::String; aws=nothing)

An S3-backed Zarr store. Available after loading the `ZarrS3AWSS3Ext`
extension, i.e. after `using AWSS3`.
"""
struct S3Store <: AbstractStore
    bucket::String
    aws::Any
end

# Without AWSS3 loaded, only the default two-argument constructor of the struct
# exists; this catch-all turns every other call into the message that says what
# to do about it. The extension adds the real `S3Store(bucket; aws=...)` method,
# which is more specific than this one and therefore wins once it is loaded.
function S3Store(args...)
    error("AWSS3 must be loaded to use S3Store. Try `using AWSS3`.")
end

# The registry lives in `ZarrCore`, so the entry has to be added at *load* time,
# not at precompile time: a mutation of another package's global state made
# while this module's body runs is discarded when the precompiled image is
# written out, and the entry would simply be missing in every fresh session.
#
# Registration happens here rather than in the extension so that
# `zopen("s3://...")` without AWSS3 loaded reaches the friendly constructor
# error above instead of "no storage type matched". `storageregexlist` ranks
# entries by specificity rather than insertion order, so a plain `push!` is
# enough and load order does not matter.
function __init__()
    push!(storageregexlist, r"^s3://" => S3Store)
end

# `S3Store` was exported by `ZarrCore` before it moved here, so it is exported
# (not just public) to keep `using Zarr; S3Store` working.
export S3Store

end # module
