module Zarr

import ZarrCore

# Mirror ZarrCore's export/public split. Internals stay at Zarr.ZarrCore.
#
# `names` reports exported *and* public names, but only from Julia 1.11 on --
# 1.10 has no `public` keyword, so ZarrCore additionally records every `@public`
# name in `PUBLIC_NAMES`. Taking the union keeps the surface identical on every
# supported version; without it, every public-but-not-exported name would be
# missing from `Zarr` on LTS.
for name in union(names(ZarrCore; all = false), ZarrCore.PUBLIC_NAMES)
    if name !== :ZarrCore
        @eval import ZarrCore: $name
    end
    if Base.isexported(ZarrCore, name) && name !== :ZarrCore
        @eval export $name
    else
        @static if VERSION >= v"1.11"
            Core.eval(@__MODULE__, Expr(:public, name))
        end
    end
end

end
