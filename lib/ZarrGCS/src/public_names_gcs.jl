# The public-but-not-exported names of `ZarrGCS`. `GCStore` itself is
# *exported* (see `ZarrGCS.jl`) because it was exported by `ZarrCore` before the
# split, so it does not belong in this list.
#
# Only bare `public` statements belong in this file: `Zarr` parses it verbatim
# on Julia 1.10, where `public` is not a keyword and `names()` cannot report
# public names. See `Zarr._declared_public_names`.

public gcs_credentials
