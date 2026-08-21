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

Subpackages re-exported by `Zarr`. Earlier modules win name collisions.
"""
const REEXPORTED_MODULES = (ZarrCore, ZarrHTTP, ZarrGCS, ZarrS3, ZarrZip, ZarrBlosc, ZarrZlib, ZarrZstd)

"""
    Zarr._public_names_file(mod) -> Union{String,Nothing}

Return the `public_names_*.jl` beside `mod`'s entry point, if present.
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

Parse public-only names from `mod`'s declaration file without evaluating it.
This supplies names that Julia 1.10 cannot report through `names`.
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

# Julia 1.10 needs the parsed public declarations in addition to exports.
@static if VERSION >= v"1.11"
    _reexported_names(mod::Module) = names(mod; all = false)
else
    _reexported_names(mod::Module) =
        union(names(mod; all = false), _declared_public_names(mod))
end

# Re-export exports; bind public-only names without exporting them.
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

# User-facing public names; Julia 1.10 uses the parsed declarations above.
@static if VERSION >= v"1.11"
    include("public_names_zarr.jl")
end

end
