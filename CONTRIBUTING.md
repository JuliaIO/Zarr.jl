# Contributors Guide

Thank you for your interest in contributing to `Zarr.jl`! We welcome everyone, whether you're new to open source, Julia, or scientific computing, or an experienced developer. Every contribution, big or small, helps make this project better.

If you have questions, ideas, or just want to chat, please reach out to us anytime. We're happy to help and discuss anything related to `Zarr.jl`.

## How You Can Contribute

* **Report bugs or suggest features:** [Open a GitHub issue](https://github.com/JuliaIO/Zarr.jl/issues/new/) to let us know about problems or ideas.
* **Start or join a discussion:** [Create a GitHub discussion](https://github.com/JuliaIO/Zarr.jl/discussions/new/choose) to ask questions, share experiences, or brainstorm.
* **Improve documentation:** Help us make our docs clearer and more helpful for everyone.
* **Write code:** Fix bugs, add features, or improve performance.

### Tips for Creating Issues

The most helpful bug reports:

* Include a clear code snippet (not just a link) that shows the problem in the latest version of `Zarr.jl`. A ["minimal working example"](https://en.wikipedia.org/wiki/Minimal_working_example) is ideal.
* Paste the full error message you received, even if it's long.
* Use triple backticks (```` ``` ````) for code, and [markdown formatting](https://docs.github.com/en/github/writing-on-github/getting-started-with-writing-and-formatting-on-github/basic-writing-and-formatting-syntax) to keep things readable.
* Share your `Zarr.jl` version, Julia version, and details about your computer or environment.

Discussions are great for questions about usage, implementation, science, or anything else.

## Ready to Start Coding?

* Fork the [`Zarr.jl` repository](https://docs.github.com/en/github/collaborating-with-pull-requests/working-with-forks), make your changes, and [open a pull request](https://docs.github.com/en/github/collaborating-with-pull-requests/proposing-changes-to-your-work-with-pull-requests/creating-a-pull-request-from-a-fork). We'll review and help you get it merged.
* For small fixes (like typos), you can use the [GitHub editor](https://docs.github.com/en/github/managing-files-in-a-repository/managing-files-on-github/editing-files-in-your-repository) for a quick edit and pull request.

## Good First Steps

### Monorepo development

The root `Project.toml` is a Julia 1.12+ workspace. Packages remain compatible
with Julia 1.10+ and live in the top-level directories `Zarr`, `ZarrCore`,
`ZarrBlosc`, `ZarrZlib`, `ZarrZstd`, `ZarrHTTP`, `ZarrGCS`, `ZarrS3`, and
`ZarrZip`. The facade source and tests are in `Zarr/src` and `Zarr/test`, while
documentation is in `docs`.

```bash
julia +1.12 --project=. -e 'using Pkg; Pkg.instantiate(; workspace=true)'
julia +1.12 --project=Zarr/test Zarr/test/v3_julia.jl
julia +1.12 --project=Zarr/test Zarr/test/v3_python.jl
julia +1.12 --project=Zarr -e 'using Pkg; Pkg.test()'
julia +1.12 --project=docs docs/make.jl
```

The two fixture commands are required before the test suite. On Julia 1.11+,
each project's `[sources]` entries resolve its local dependencies. On Julia
1.10, develop sibling packages with explicit relative paths, such as
`Pkg.develop(path="../ZarrCore")`.

* Try out `Zarr.jl` using the examples in our documentation, or create your own. If you hit any problems or have questions, please open an issue!
* Write an example or tutorial showing how to use `Zarr.jl` for something interesting.
* Suggest improvements to documentation or comments.
* Implement a new feature you’d like to see.

If you want to work on something, let us know by commenting on an issue or opening a new one. This helps us coordinate and support you.

We’re excited to have you join our community. Thank you for helping make `Zarr.jl` better!
