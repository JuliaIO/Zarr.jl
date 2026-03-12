```@raw html
---
# https://vitepress.dev/reference/default-theme-home-page
layout: home

hero:
  name: "Zarr.jl"
  text: "A Julia library for the Zarr storage format"
  tagline: Reading and Writing Zarr Datasets from Julia
  actions:
    - theme: brand
      text: Tutorial
      link: ./tutorial.md
    - theme: brand
      text: Reference
      link: ./reference.md

features:
  - title: Zarr specs v3
    details: Incomplete implementation
    link: https://zarr-specs.readthedocs.io/en/latest/v3/core/index.html
  - title: Zarr specs v2
    details: Incomplete implementation
    link: https://zarr-specs.readthedocs.io/en/latest/v2/v2.0.html
  - title: Read and write Zarr datasets
    details: It is possible to read an write (compressed) chunked n-dimensional arrays to disk, memory and cloud storage backends.
    link: ./tutorial.md
---
```