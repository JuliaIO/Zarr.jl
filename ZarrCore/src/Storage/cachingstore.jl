"""
    CachingStore{R<:AbstractStore,C<:AbstractStore} <: AbstractStore

A store that caches reads from a `remote` store in a local `cache` store.
On first access, chunks are fetched from the remote store and written to
the cache. Subsequent accesses read from the cache.

This is useful for large remote datasets where you want to avoid
re-downloading data on every access, while still benefiting from
cloud-hosted data.

Like HTTPStore, this store requires consolidated metadata and will be
wrapped in a ConsolidatedStore when opened via `zopen`.

# Example
```julia
store = CachingStore(
    "https://storage.googleapis.com/some-zarr-store",
    "/path/to/local/cache"
)
g = zopen(store)  # automatically wraps in ConsolidatedStore
```
"""
struct CachingStore{R<:AbstractStore,C<:AbstractStore} <: AbstractStore
    remote::R
    cache::C
end

function CachingStore(url::AbstractString, cache_path::AbstractString)
    CachingStore(first(storefromstring(url)), first(storefromstring(cache_path)))
end

function Base.getindex(s::CachingStore, k::AbstractString)
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
Base.setindex!(s::CachingStore, v, k::AbstractString) = s.cache[k] = v

# Delete from cache
Base.delete!(s::CachingStore, k::AbstractString) = delete!(s.cache, k)

# Delegate to cache for discovery (after consolidated metadata is cached)
subdirs(s::CachingStore, p) = subdirs(s.cache, p)
subkeys(s::CachingStore, p) = subkeys(s.cache, p)

# Storage size from cache
storagesize(s::CachingStore, p) = storagesize(s.cache, p)

# Use the remote store's read strategy
store_read_strategy(s::CachingStore) = store_read_strategy(s.remote)

# Forward missing chunk codes to remote
missing_chunk_return_code!(s::CachingStore, code) = missing_chunk_return_code!(s.remote, code)
has_configurable_missing_chunks(::CachingStore) = true

Base.show(io::IO, ::CachingStore) = print(io, "Caching Storage")
