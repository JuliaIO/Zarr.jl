module Zarr

import ZarrCore

# Mirror ZarrCore's export/public split. Internals stay at Zarr.ZarrCore.
for name in names(ZarrCore; all = false)
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
