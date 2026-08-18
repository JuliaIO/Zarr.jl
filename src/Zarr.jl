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

# Mirror each subpackage's export/public split. Internals stay at
# `Zarr.<Subpackage>.foo`.
#
# `names` reports exported *and* public names, but only from Julia 1.11 on --
# 1.10 has no `public` keyword, so every subpackage additionally records its
# `@public` names in its own `PUBLIC_NAMES`. Taking the union keeps the surface
# identical on every supported version; without it, every public-but-not-exported
# name would be missing from `Zarr` on LTS.
let seen = Set{Symbol}()
    for mod in REEXPORTED_MODULES
        modname = nameof(mod)
        push!(seen, modname)
        @static if VERSION >= v"1.11"
            Core.eval(@__MODULE__, Expr(:public, modname))
        end
        for name in union(names(mod; all = false), mod.PUBLIC_NAMES)
            name in seen && continue
            push!(seen, name)
            @eval import $modname: $name
            if Base.isexported(mod, name)
                @eval export $name
            else
                @static if VERSION >= v"1.11"
                    Core.eval(@__MODULE__, Expr(:public, name))
                end
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

end
