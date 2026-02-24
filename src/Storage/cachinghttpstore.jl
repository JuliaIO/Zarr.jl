"""
    CachingHTTPStore <: AbstractStore

An HTTP store that caches chunks locally. On first access, chunks are
downloaded from the remote URL and saved to the local cache directory.
Subsequent accesses read from the local cache.

This is useful for large remote datasets where you want to avoid
re-downloading data on every access, while still benefiting from
cloud-hosted data.

Like HTTPStore, this store requires consolidated metadata and will be
wrapped in a ConsolidatedStore when opened via `zopen`.

# Example
```julia
store = CachingHTTPStore(
    "https://storage.googleapis.com/some-zarr-store",
    "/path/to/local/cache"
)
g = zopen(store)  # automatically wraps in ConsolidatedStore
```
"""
struct CachingHTTPStore <: AbstractStore
    remote::HTTPStore
    cache::DirectoryStore
end

function CachingHTTPStore(url::String, cache_path::String)
    CachingHTTPStore(HTTPStore(url), DirectoryStore(cache_path))
end

function Base.getindex(s::CachingHTTPStore, k::String)
    # Check cache first
    cached = s.cache[k]
    if cached !== nothing
        return cached
    end
    # Fetch from remote
    data = s.remote[k]
    if data !== nothing
        # Cache it
        s.cache[k] = data
    end
    return data
end

# Write to cache (for metadata etc)
Base.setindex!(s::CachingHTTPStore, v, k::String) = s.cache[k] = v

# Delete from cache
Base.delete!(s::CachingHTTPStore, k::String) = delete!(s.cache, k)

# Delegate to cache for discovery (after consolidated metadata is cached)
subdirs(s::CachingHTTPStore, p) = subdirs(s.cache, p)
subkeys(s::CachingHTTPStore, p) = subkeys(s.cache, p)

# Storage size from cache
storagesize(s::CachingHTTPStore, p) = storagesize(s.cache, p)

# Use concurrent reads like HTTPStore
store_read_strategy(s::CachingHTTPStore) = store_read_strategy(s.remote)

# Forward missing chunk codes to remote
missing_chunk_return_code!(s::CachingHTTPStore, code) = missing_chunk_return_code!(s.remote, code)
has_configurable_missing_chunks(::CachingHTTPStore) = true

Base.show(io::IO, ::CachingHTTPStore) = print(io, "Caching HTTP Storage")

# Register so zopen wraps in ConsolidatedStore like HTTPStore
storefromstring(::Type{CachingHTTPStore}, url, cache_path) = ConsolidatedStore(CachingHTTPStore(url, cache_path), ""), ""
