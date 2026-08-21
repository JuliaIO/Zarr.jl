# The public-but-not-exported names of `ZarrZlib`, moved here from `ZarrCore`'s
# list when the compressors were split out. The v2 and v3 specs spell the same
# algorithm differently, hence `ZlibCompressor` next to `GzipV3Codec`.
#
# Only bare `public` statements belong in this file: `Zarr` parses it verbatim
# on Julia 1.10, where `public` is not a keyword and `names()` cannot report
# public names. See `Zarr._declared_public_names`.

public ZlibCompressor, GzipV3Codec
