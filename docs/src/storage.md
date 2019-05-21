# Developing new storage backends

One advantage of the zarr data model is that it can be used in combiantion with a variety of storage backends. Currently in this package there is support for a `DictStore` (keeping data in memory), `DirectoryStore` (writing data to a local disk) and an `S3Store` for S3-compatible object store which is currently read-only. In oder to implement a new storage backend, you would have to create a subtype of `Zarr.AbstractStore` and implement the following methods:
```@meta
CurrentModule = Zarr
```


```@docs
storagesize
zname
Base.getindex(d::AbstractStore,i::String)
Base.setindex!(d::AbstractStore,v,i::String)
subdirs
Base.keys(d::AbstractStore)
newsub
getsub
```

You can get some inspiration on how to implement this by looking at the source code of existing storage backends.
