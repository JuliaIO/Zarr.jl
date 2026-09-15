# ZarrHTTP.jl

Read-only HTTP/HTTPS storage and HTTP serving for Zarr arrays and groups.

## What it exposes

- `ZarrHTTP.HTTPStore(url, allowed_codes=Set((404,)))`: a read-only store
  whose keys are fetched relative to `url`.
- `ZarrHTTP.register!()`: enables `zopen("https://…")` and `zopen("http://…")`.
  This runs automatically unless ZarrCore's `RegisterAtInit` preference is disabled.
- Methods for `ZarrCore.missing_chunk_return_code!(store, code)` (or a vector
  of codes), to treat additional HTTP statuses as missing keys.
- Methods for `HTTP.serve` and `HTTP.serve!` accepting a `ZArray`, `ZGroup`,
  or a store plus its path. Import `HTTP` explicitly to call these functions.

`HTTPStore` and `register!` are public but not exported. URL-based opening
uses consolidated metadata when available; HTTP stores cannot list directories,
so remote groups need consolidated metadata for child discovery.

## Quick example

Serve an in-memory array locally, read it over HTTP, and close the server:

```julia
using ZarrCore, ZarrHTTP
import HTTP

a = zcreate(Int32, 4; chunks=(2,))
a[:] = Int32[10, 20, 30, 40]

server = HTTP.serve!(a, "127.0.0.1", 8080)
try
    b = zopen("http://127.0.0.1:8080", "r")
    @assert b[2:3] == Int32[20, 30]
finally
    close(server)
end
```

This example requires the `HTTP` package and an available local port 8080.

[Main README](../README.md) · [Documentation](https://juliaio.github.io/Zarr.jl/) · [License](../LICENSE.md)
