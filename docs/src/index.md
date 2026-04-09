```@raw html
---
# https://vitepress.dev/reference/default-theme-home-page
layout: home

hero:
  name: "Zarr.jl"
  text: "A Julia library for the Zarr storage format"
  tagline: Chunked, compressed N-dimensional arrays for every scale and storage backend
  actions:
    - theme: brand
      text: Get Started
      link: ./get_started.md
    - theme: alt
      text: View on GitHub
      link: https://github.com/JuliaIO/Zarr.jl
    - theme: alt
      text: API Reference
      link: ./reference.md

features:
  - title: Zarr v3 & v2 Support
    details: Compatible with both Zarr v2 and Zarr v3 formats, with examples provided for each version wherever possible to illustrate equivalent usage and highlight any differences between them.
    link: ./get_started

  - title: Chunked Arrays & Codecs
    details: Slice through terabytes effortlessly, tune chunk shapes and pick from a rich set of compression codecs to match your access patterns and storage constraints.
    link: ./get_started#Compression

  - title: Flexible Storage Backends
    details: Read and write to local disk or in-memory stores. S3-compatible object storage is supported for reading. New backends can be added by subtyping <a class="highlight-link">Zarr.AbstractStore</a>.
    link: ./UserGuide/storage

  - title: Groups & Hierarchies
    details: Structure your data the way you think about it, nest arrays into groups and build intuitive hierarchies for any dataset complexity.
    link: ./get_started#Groups

  - title: YAXArrays.jl Integration
    details: <a class="highlight-link">Zarr.jl</a> integrates seamlessly with <a class="highlight-link">YAXArrays.jl</a>, providing the storage layer for labeled, multi-dimensional data.
    link: https://juliadatacubes.github.io/YAXArrays.jl/stable/

  - title: Sharding - Coming Soon!
    details: Support for the Zarr v3 sharding codec is on the roadmap, enabling efficient storage of many chunks within a single file.
---
```