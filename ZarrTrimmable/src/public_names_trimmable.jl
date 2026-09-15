# Parsed by `Zarr` on Julia 1.10; keep only `public` statements and comments.

# Generated-reader descriptor; the macro itself is exported.
public ZarrReader

# Closed-set element types.
public DType, DFloat64, DFloat32, DInt32, DInt64, DUInt8
public julia_type, dtype_from_typestr, typestr

# Closed-set compressor descriptions (pure metadata).
public CompressorSpec, NoComp, Zstd, Zlib, Blosc
public compressor_id, compressor_from_json

# Closed-set fill-value encodings.
public FillValue, FillNone, FillNumber, FillNaN, FillInt, fill_from_json

# The `.zarray` mirror and its I/O.
public ArrayMeta, read_meta, read_meta_path, write_meta, write_meta_string, describe
