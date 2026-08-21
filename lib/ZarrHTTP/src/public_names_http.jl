# The public-but-not-exported names of `ZarrHTTP`, moved here from `ZarrCore`'s
# list when the HTTP backend was split out.
#
# Only bare `public` statements belong in this file: `Zarr` parses it verbatim
# on Julia 1.10, where `public` is not a keyword and `names()` cannot report
# public names. See `Zarr._declared_public_names`.

public HTTPStore
