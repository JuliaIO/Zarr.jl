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

Read a few latitude coordinates from the public CMIP6 dataset used by this
repository's tests. No Google Cloud credentials are needed:

```julia
using ZarrCore, ZarrGCS, ZarrBlosc

url = "gs://cmip6/CMIP6/HighResMIP/CMCC/CMCC-CM2-HR4/" *
      "highresSST-present/r1i1p1f1/6hrPlev/psl/gn/v20170706"
g = zopen(url, "r")
g["lat"][1:4] # approximately [-90.0, -89.0576, -88.1152, -87.1728]
```

This example requires internet access and loads `ZarrBlosc` to decode compressed
chunks. For private data, call
`ZarrGCS.gcs_credentials("my-project", ENV["GCS_ACCESS_TOKEN"], "Bearer")`
before opening the dataset.

[Main README](../README.md) · [Documentation](https://juliaio.github.io/Zarr.jl/) · [License](../LICENSE.md)
