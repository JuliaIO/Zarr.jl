# ZarrGCS.jl

Read-only Google Cloud Storage access for Zarr arrays and groups.

## What it exposes

- `GCStore(url::String)` (exported): a store for a GCS bucket. Use `zopen`
  with a full URL to resolve both the bucket and the dataset path.
- `ZarrGCS.gcs_credentials(user_project, access_token, token_type)`: sets
  credentials and the billing project for subsequent GCS requests.
- `ZarrGCS.gcs_credentials(; metadata_url=...)`: loads credentials from a
  metadata server, defaulting to the Google Compute Engine metadata endpoint.
- `ZarrGCS.register!()`: registers `gs://` and the HTTP/HTTPS
  `storage.googleapis.com` URL prefixes with ZarrCore. This runs automatically
  unless ZarrCore's `RegisterAtInit` preference is disabled.

## Quick example

Replace the bucket and path with an existing Zarr array you can access:

```julia
using ZarrCore, ZarrGCS

# For private data, configure credentials before opening:
# ZarrGCS.gcs_credentials("my-project", ENV["GCS_ACCESS_TOKEN"], "Bearer")

a = zopen("gs://my-bucket/data.zarr", "r")
size(a)
```

Public data can be read without credentials. Load the appropriate compressor
package (such as `ZarrBlosc`) before reading compressed chunks.

[Main README](../README.md) · [Documentation](https://juliaio.github.io/Zarr.jl/) · [License](../LICENSE.md)
