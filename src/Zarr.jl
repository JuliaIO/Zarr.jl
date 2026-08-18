module Zarr

import ZarrCore
import ZarrHTTP
import ZarrGCS
import ZarrS3
import ZarrZip
import ZarrBlosc
import ZarrZlib
import ZarrZstd

"""
    Zarr.REEXPORTED_MODULES

The subpackages whose API `Zarr` mirrors, in precedence order.

`Zarr` itself holds no implementation: every type and method lives in one of
these packages, and the loop below re-exports their API so that `using Zarr`
behaves as if it were a single package. Adding a subpackage is a one-line
change here (plus the matching `import` above and a dep in `Project.toml`).

Ordering matters only for the pathological case of two subpackages declaring
the same name: the first module listed wins, and the later binding is not
imported. `ZarrCore` is therefore first. Subpackages are expected to keep their
names distinct; a collision is a bug in the subpackage, not something the
facade should paper over silently.

Each module's own name (`Zarr.ZarrCore`, `Zarr.ZarrZip`, ...) stays reachable as
the documented escape hatch to internals: public, but never exported.
"""
const REEXPORTED_MODULES = (ZarrCore, ZarrHTTP, ZarrGCS, ZarrS3, ZarrZip, ZarrBlosc, ZarrZlib, ZarrZstd)

"""
    Zarr._public_names_file(mod) -> Union{String,Nothing}

Path of the `public_names_*.jl` that `mod` keeps next to its entry point, or
`nothing` if it has none.

Every package declares its public-but-not-exported names in exactly one such
file (`public_names_core.jl`, `public_names_zip.jl`, ...), so the lookup is a
`readdir` of the module's own source directory and needs no registration.
"""
function _public_names_file(mod::Module)
    local dir
    if mod === @__MODULE__
        dir = @__DIR__
    else
        path = pathof(mod)
        path === nothing && return nothing
        dir = dirname(path)
    end
    isdir(dir) || return nothing
    for f in readdir(dir)
        if startswith(f, "public_names_") && endswith(f, ".jl")
            return joinpath(dir, f)
        end
    end
    return nothing
end

"""
    Zarr._declared_public_names(mod) -> Vector{Symbol}

The public-but-not-exported names `mod` declares in its `public_names_*.jl`.

Those files hold nothing but bare `public` statements and are `include`d only
under `@static if VERSION >= v"1.11"`, because `public` is not a keyword before
that -- Julia 1.10 can neither parse the statements nor report the names through
`names(mod)`. This function recovers them anyway: it rewrites the leading
`public` of each statement into a macro call (same shape, parseable on every
version), parses the result, and reads the symbols straight off the expression.
Nothing is evaluated, and no macro named in the rewrite has to exist.

It is what keeps the facade identical on LTS and on 1.11+: the re-export loop
below uses it in place of the public half of `names(mod)`, which is empty on
1.10. Without it every public-but-not-exported name would silently vanish from
`Zarr` there.
"""
function _declared_public_names(mod::Module)
    file = _public_names_file(mod)
    file === nothing && return Symbol[]
    src = replace(read(file, String), r"(?m)^public\b" => "@_public_names")
    syms = Symbol[]
    for stmt in Meta.parseall(src).args
        stmt isa Expr && stmt.head === :macrocall || continue
        stmt.args[1] === Symbol("@_public_names") || continue
        arg = stmt.args[3]
        if arg isa Symbol
            push!(syms, arg)
        elseif arg isa Expr && arg.head === :tuple
            append!(syms, Iterators.filter(a -> a isa Symbol, arg.args))
        end
    end
    return syms
end

# The names `Zarr` takes over from one subpackage: everything `names` reports
# on 1.11+, and the same set reconstructed from the declaration file on 1.10.
@static if VERSION >= v"1.11"
    _reexported_names(mod::Module) = names(mod; all = false)
else
    _reexported_names(mod::Module) =
        union(names(mod; all = false), _declared_public_names(mod))
end

# Mirror each subpackage's export/public split. Internals stay at
# `Zarr.<Subpackage>.foo`.
#
# Exports are re-exported as exports. Everything else is only *imported*, so the
# binding exists (`Zarr.typestr` works) without `using Zarr` dragging it into
# scope; which of those names `Zarr` itself advertises as public is decided by
# `public_names_zarr.jl` at the bottom of this file, and is deliberately a
# smaller, user-facing subset of the extension API the subpackages declare.
let seen = Set{Symbol}()
    for mod in REEXPORTED_MODULES
        modname = nameof(mod)
        push!(seen, modname)
        @static if VERSION >= v"1.11"
            Core.eval(@__MODULE__, Expr(:public, modname))
        end
        for name in _reexported_names(mod)
            name in seen && continue
            push!(seen, name)
            @eval import $modname: $name
            if Base.isexported(mod, name)
                @eval export $name
            end
        end
    end
end

# `ZarrCore` on its own knows only `NoCompressor`, so the Blosc default that
# `using Zarr` has always had is installed here, by the one package that is
# guaranteed to have `ZarrBlosc` loaded.
#
# This has to happen at *load* time rather than at precompile time: a mutation
# of another package's global state made while this module's body runs is
# discarded when the precompiled image is written out.
function __init__()
    ZarrCore.DEFAULT_COMPRESSOR[] = ZarrBlosc.BloscCompressor()
end

# `Zarr`'s own public API: the user-facing subset of what the subpackages
# declare. Nothing to do on 1.10, which has no `public` -- the bindings are
# already in place, and `names()` could not report them either way.
@static if VERSION >= v"1.11"
    include("public_names_zarr.jl")
end

end
