module Zarr

import ZarrCore

# Mirror ZarrCore's export/public split. Internals stay at Zarr.ZarrCore.

for name in names(ZarrCore)
    if name !== :ZarrCore
        @eval import ZarrCore: $name
    
        if Base.isexported(ZarrCore, name)
            @eval export $name
        end
    end
end

@static if VERSION >= v"1.11"
    include("public_names_zarr.jl")
else
    # For Julia 1.10, we have to parse the public names from the source file, since
    # `public` is not a keyword and `names(ZarrCore)` only returns exported names.
    let public_names = read(joinpath(@__DIR__, "..", "lib", "ZarrCore", "public_names_zarr.jl"), String)
        public_names = replace(public_names, "public" => "using ZarrCore: ")
        eval(Meta.parseall(public_names))
    end
end

end
