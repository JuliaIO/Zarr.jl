#!/usr/bin/env python3
"""Generate a small test zarr file with big-endian data (zarr v2 format)."""

import numpy as np
import zarr
import shutil
from pathlib import Path

# Output directory
output_dir = Path(__file__).parent / "big_endian_test.zarr"

# Clean up if exists
if output_dir.exists():
    shutil.rmtree(output_dir)

# Create a group with a named array with big-endian int16 dtype (zarr v2 format)
data = np.arange(10, dtype=">i2")
root = zarr.open_group(str(output_dir), mode="w", zarr_format=2)
z_write = root.create_array("big_endian_var", shape=10, dtype=">i2", chunks=10)
z_write[:] = data

print("=== Write ===")
print(f"dtype: {z_write.dtype}")
print(f"data: {z_write[:]}")

# Read it back
root_read = zarr.open_group(str(output_dir), mode="r")
z_read = root_read["big_endian_var"]

print("\n=== Read ===")
print(f"dtype: {z_read.dtype}")
print(f"data: {z_read[:]}")

print(f"\nCreated: {output_dir}")
