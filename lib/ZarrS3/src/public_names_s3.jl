# `ZarrS3` deliberately declares no public-but-not-exported names: `S3Store` was
# exported by `ZarrCore` before the split and stays *exported* (see `ZarrS3.jl`),
# and every method lives in the AWSS3 extension. The file exists so that every
# subpackage has one place to look; add `public` statements here if that changes.
#
# Only bare `public` statements belong in this file: `Zarr` parses it verbatim
# on Julia 1.10, where `public` is not a keyword and `names()` cannot report
# public names. See `Zarr._declared_public_names`.
