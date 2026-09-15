# pools.jl -- `@zarr_reader`: code-generated, strongly typed Zarr readers.
#
# The closed set of a reader is described by four *pools*:
#
#     reader = @zarr_reader(dtypes = (Float64, Int32), ndims = (2, 3),
#                           codecs = (ZarrCore.NoCompressor, ZstdCompressor,
#                                     ZlibCompressor, BloscCompressor),
#                           stores = (DirectoryStore,), tag = R1)
#
# The macro emits, in the *caller's* module:
#
#   * `CodecPool_R1 <: ZarrCore.Compressor` -- one field whose type is the
#     `Union` of the codec pool, plus the whole compressor interface forwarded
#     with explicit `if x isa A ... elseif x isa B ...` chains.
#   * `StorePool_R1 <: ZarrCore.AbstractStore` -- likewise for the store
#     interface.
#   * methods of `ZarrCore.zopen` / `ZarrCore.zcreate` on the singleton
#     `ZarrReader{CodecPool_R1,StorePool_R1,Tuple{Float64,Int32},Tuple{Val{2},Val{3}}}`.
#
# Why explicit chains and not multimethods: a field typed `Union{A,B,C,D,...}`
# is only union-split by the compiler up to a small limit (and `tmerge` collapses
# a merge of more than `Base.Compiler.MAX_TYPEUNION_LENGTH = 3` types), so
# `zuncompress!(a, b, p.c)` on a wide union becomes a runtime dispatch, which
# `--trim=safe` rejects. An `isa` chain keeps every call site statically
# resolved no matter how wide the union is; each branch is its own concrete
# call.
#
# Only |dtypes| x |ndims| `ZArray` specialisations are ever instantiated: the
# codec and the store are *values* inside the pool structs, not type parameters
# of `ZArray`, so growing those pools costs no new `ZArray` variants.

# ---------------------------------------------------------------------------
# reader value (a type-level descriptor; no fields)
# ---------------------------------------------------------------------------

"""
    ZarrReader{CP,SP,DT<:Tuple,ND<:Tuple}

Singleton descriptor of a generated reader: `CP` is the Zarr v2 codec-pool
struct (or `Nothing` for a v3-only reader that was given no `codecs` pool),
`SP` the store-pool struct, `DT` a `Tuple{...}` of the element types and `ND` a
`Tuple{Val{2},...}` of the ranks in the pool. Build one with
[`@zarr_reader`](@ref); the methods that do the work are emitted by the macro
for this exact parametrisation.
"""
struct ZarrReader{CP,SP,DT<:Tuple,ND<:Tuple} end

dtypes(::ZarrReader{CP,SP,DT,ND}) where {CP,SP,DT,ND} = DT
ndim_pool(::ZarrReader{CP,SP,DT,ND}) where {CP,SP,DT,ND} = ND
codecpool(::ZarrReader{CP,SP,DT,ND}) where {CP,SP,DT,ND} = CP
storepool(::ZarrReader{CP,SP,DT,ND}) where {CP,SP,DT,ND} = SP

# Concrete-in / `String`-out: specialised on the concrete lowered value of one
# codec, so `JSON.print` is statically typed in every branch.
function _json_string(x)
    io = IOBuffer()
    JSON.print(io, x)
    return String(take!(io))
end

const _READER_COUNTER = Ref(0)

# ---------------------------------------------------------------------------
# expression builder
# ---------------------------------------------------------------------------

_pool_items(v) = (v isa Expr && v.head === :tuple) ? collect(v.args) : Any[v]

_chain(pairs::Vector{Any}, elsebody) = _chain_from(pairs, 1, elsebody)
function _chain_from(pairs::Vector{Any}, i::Int, elsebody)
    i > length(pairs) && return elsebody
    cond, body = pairs[i]
    return Expr(:if, cond, body, _chain_from(pairs, i + 1, elsebody))
end

"""
    _zarr_reader_expr(dtypes, ndims, codecs, stores, tag;
                      path_store, read_strategy, formats, v3codecs)

Build the `Expr` that [`@zarr_reader`](@ref) splices in. `dtypes`, `codecs`,
`stores` and `v3codecs` are vectors of *expressions* naming types (resolved in
the caller's module), `ndims` a vector of integer literals, `formats` a vector
of the Zarr format versions (drawn from `2` and `3`) the reader accepts, and
`tag` a `Symbol` used to derive the generated names.
"""
function _zarr_reader_expr(dtypes::Vector, nds::Vector, codecs::Vector, stores::Vector,
                           tag::Symbol; path_store = nothing,
                           read_strategy::Symbol = :sequential,
                           formats::Vector{Int} = Int[2],
                           v3codecs::Vector = Any[])
    isempty(dtypes) && error("@zarr_reader: `dtypes` pool is empty")
    isempty(nds)    && error("@zarr_reader: `ndims` pool is empty")
    isempty(stores) && error("@zarr_reader: `stores` pool is empty")
    isempty(formats) && error("@zarr_reader: `zarr_format` pool is empty")
    all(v -> v == 2 || v == 3, formats) ||
        error("@zarr_reader: `zarr_format` must be drawn from `(2,)`, `(3,)` and `(2, 3)`")
    length(unique(formats)) == length(formats) ||
        error("@zarr_reader: `zarr_format` has a repeated entry")
    has2 = 2 in formats
    has3 = 3 in formats
    has2 && isempty(codecs) && error("@zarr_reader: `codecs` pool is empty")
    has3 && isempty(v3codecs) &&
        error("@zarr_reader: `v3_codecs` pool is empty; a reader with 3 in `zarr_format` ",
              "needs at least one `V3Codec{:bytes,:bytes}` type")
    !has3 && !isempty(v3codecs) &&
        error("@zarr_reader: `v3_codecs` was given but 3 is not in `zarr_format`")
    all(n -> n isa Integer, nds) || error("@zarr_reader: `ndims` must be integer literals")
    (read_strategy === :sequential || read_strategy === :forward) ||
        error("@zarr_reader: `read_strategy` must be `sequential` or `forward`, got ",
              read_strategy)

    ZC  = ZarrCore          # module objects, interpolated -> hygiene-proof
    V3C = ZarrCore.Codecs.V3Codecs
    JS  = JSON
    ZT  = @__MODULE__

    CP  = Symbol("CodecPool_", tag)
    BB  = Symbol("BBPool_", tag)
    PL  = Symbol("P_", tag)
    SP  = Symbol("StorePool_", tag)
    RT  = Symbol("ReaderType_", tag)
    AU  = Symbol("ZArrayUnion_", tag)
    fleaf = Symbol("_pool_leaf_", tag)
    fnd   = Symbol("_pool_nd_", tag)
    gleaf = Symbol("_pool_ret_leaf_", tag)
    gnd   = Symbol("_pool_ret_nd_", tag)
    fleaf3 = Symbol("_pool_leaf3_", tag)
    fnd3   = Symbol("_pool_nd3_", tag)
    gleaf3 = Symbol("_pool_ret_leaf3_", tag)
    gnd3   = Symbol("_pool_ret_nd3_", tag)

    pstore = path_store === nothing ? stores[1] : path_store

    # The v2 codec pool (and with it the v2 `zcreate` methods) is emitted
    # whenever a `codecs` pool was given. A v3-only reader may leave `codecs`
    # empty, and then gets neither.
    emit_v2_codecs = !isempty(codecs)

    out = Expr(:block)

    # ---- codec pool -------------------------------------------------------
    if emit_v2_codecs
    push!(out.args, :(struct $CP <: $ZC.Compressor
                          c::Union{$(codecs...)}
                      end))

    # zuncompress!(data, compressed, pool)
    unc = _chain(Any[(:(c isa $M), quote
                         $ZC.zuncompress!(data, compressed, c)
                         return nothing
                     end) for M in codecs],
                 :(throw(ArgumentError("codec pool member not handled"))))
    push!(out.args, :(function $ZC.zuncompress!(data, compressed, p::$CP)
                          c = p.c
                          $unc
                      end))
    # zcompress!(compressed, data, pool)
    cmp = _chain(Any[(:(c isa $M), quote
                          $ZC.zcompress!(compressed, data, c)
                          return nothing
                      end) for M in codecs],
                 :(throw(ArgumentError("codec pool member not handled"))))
    push!(out.args, :(function $ZC.zcompress!(compressed, data, p::$CP)
                          c = p.c
                          $cmp
                      end))
    # JSON.lower: every branch renders one concrete lowered codec to a
    # String, so the return type is always `JSON.JSONText` -- a chain
    # returning the raw NamedTuples would tmerge to `Any`.
    low = _chain(Any[(:(c isa $M), :(return $JS.JSONText($ZT._json_string($JS.lower(c)))))
                     for M in codecs],
                 :(throw(ArgumentError("codec pool member not handled"))))
    push!(out.args, :(function $JS.lower(p::$CP)
                          c = p.c
                          $low
                      end))

    # getCompressor(::Type{CP}, ::Nothing) -- the member whose id is ""
    gnone = _chain(Any[(:($ZC.codec_id($M) == ""),
                        :(return $CP($ZC.getCompressor($M, nothing)))) for M in codecs],
                   :(throw(ArgumentError("the store has no compressor, but this reader's codec pool has no NoCompressor member"))))
    push!(out.args, :(function $ZC.getCompressor(::Type{$CP}, ::Nothing)
                          $gnone
                      end))

    gjson = _chain(Any[(:(id == $ZC.codec_id($M)),
                        :(return $CP($ZC.getCompressor($M, c)))) for M in codecs],
                   :(throw(ArgumentError(string("compressor id \"", id,
                        "\" is not in this reader's codec pool")))))
    push!(out.args, :(function $ZC.getCompressor(::Type{$CP}, c::$ZC.CompressorJSON)
                          id = c.id
                          $gjson
                      end))
    end  # emit_v2_codecs

    # ---- v3 bytes->bytes codec pool ---------------------------------------
    #
    # The v3 counterpart of `CodecPool`: one `V3Codec{:bytes,:bytes}` struct
    # whose single field is the `Union` of the pool, with the same explicit
    # `isa` chains. Because the pool member is a *value*, a pipeline can hold a
    # `Vector{BBPool}` and so accept any number of bytes->bytes codecs in any
    # order without a new `ZArray` specialisation -- which is exactly what
    # `pipeline_from_json` supports for an `AbstractVector` bytes->bytes stage.
    if has3
        push!(out.args, :(struct $BB <: $V3C.V3Codec{:bytes,:bytes}
                              c::Union{$(v3codecs...)}
                          end))

        # `::Vector{UInt8}` return annotations: without them an N-way chain over
        # codecs whose `codec_encode` bodies infer to slightly different
        # `Vector{UInt8}`-alikes would tmerge, and `pipeline_encode`'s
        # accumulator would stop being concrete.
        enc3 = _chain(Any[(:(c isa $M), :(return $V3C.codec_encode(c, data)))
                          for M in v3codecs],
                      :(throw(ArgumentError("v3 codec pool member not handled"))))
        push!(out.args, :(function $V3C.codec_encode(p::$BB, data::Vector{UInt8})::Vector{UInt8}
                              c = p.c
                              $enc3
                          end))
        dec3 = _chain(Any[(:(c isa $M), :(return $V3C.codec_decode(c, encoded)))
                          for M in v3codecs],
                      :(throw(ArgumentError("v3 codec pool member not handled"))))
        push!(out.args, :(function $V3C.codec_decode(p::$BB, encoded::Vector{UInt8})::Vector{UInt8}
                              c = p.c
                              $dec3
                          end))
        # As for the v2 pool: every branch renders one concrete lowered codec to
        # a `String`, so the return type is always `JSON.JSONText`.
        low3 = _chain(Any[(:(c isa $M), :(return $JS.JSONText($ZT._json_string($JS.lower(c)))))
                          for M in v3codecs],
                      :(throw(ArgumentError("v3 codec pool member not handled"))))
        push!(out.args, :(function $JS.lower(p::$BB)
                              c = p.c
                              $low3
                          end))
        nm3 = _chain(Any[(:(c isa $M), :(return $V3C.name(c))) for M in v3codecs],
                     :(throw(ArgumentError("v3 codec pool member not handled"))))
        push!(out.args, :(function $V3C.name(p::$BB)::String
                              c = p.c
                              $nm3
                          end))
        # The typed construction hook `pipeline_from_json` calls for each
        # trailing bytes->bytes entry of the stored chain.
        g3 = _chain(Any[(:(nm == $V3C.codec_name($M)),
                         :(return $BB($V3C.getCodec($M, c, ctx)))) for M in v3codecs],
                    :(throw(ArgumentError(string("v3 codec \"", nm,
                         "\" is not in this reader's v3 codec pool")))))
        push!(out.args, :(function $V3C.getCodec(::Type{$BB}, c::$ZC.CodecJSON, ctx)
                              nm = $V3C._strip_numcodecs(c.name)
                              $g3
                          end))
        # The pipeline type the v3 leaves ask for: no array->array stage, the
        # plain `bytes` array->bytes codec, and a run-time-length vector of pool
        # members. A stored transpose / sharding / vlen-utf8 chain fails inside
        # `pipeline_from_json` with an `ArgumentError`.
        push!(out.args, :(const $PL = $ZC.V3Pipeline{Tuple{},$ZC.BytesCodec,Vector{$BB}}))
    end

    # ---- store pool -------------------------------------------------------
    push!(out.args, :(struct $SP <: $ZC.AbstractStore
                          s::Union{$(stores...)}
                      end))

    _schain(body) = _chain(Any[(:(s isa $M), body(:s, M)) for M in stores],
                           :(throw(ArgumentError("store pool member not handled"))))

    # `String(i)`: DirectoryStore's three primitives are declared `::String`
    # while the generic 2-argument store forms hand on whatever `_concatpath`
    # produced. `String(::String)` is the identity, so this folds away when the
    # key is already a `String`.
    # The `::Union{Nothing,Vector{UInt8}}` return annotation keeps an N-way
    # chain from widening (every shipped store returns exactly that).
    push!(out.args, :(function Base.getindex(p::$SP, i::AbstractString)::Union{Nothing,Vector{UInt8}}
                          s = p.s
                          k = String(i)
                          $(_schain((v, M) -> :(return $v[k])))
                      end))
    push!(out.args, :(function Base.setindex!(p::$SP, v, i::AbstractString)
                          s = p.s
                          k = String(i)
                          $(_schain((sv, M) -> quote
                                setindex!($sv, v, k)
                                return nothing
                            end))
                      end))
    push!(out.args, :(function Base.delete!(p::$SP, i::AbstractString)
                          s = p.s
                          k = String(i)
                          $(_schain((v, M) -> quote
                                delete!($v, k)
                                return nothing
                            end))
                      end))
    push!(out.args, :(function $ZC.isinitialized(p::$SP, i::AbstractString)::Bool
                          s = p.s
                          k = String(i)
                          $(_schain((v, M) -> :(return $ZC.isinitialized($v, k))))
                      end))
    push!(out.args, :(function $ZC.subdirs(p::$SP, i)
                          s = p.s
                          $(_schain((v, M) -> :(return $ZC.subdirs($v, i))))
                      end))
    push!(out.args, :(function $ZC.subkeys(p::$SP, i)
                          s = p.s
                          $(_schain((v, M) -> :(return $ZC.subkeys($v, i))))
                      end))
    push!(out.args, :(function $ZC.storagesize(p::$SP, i)
                          s = p.s
                          $(_schain((v, M) -> :(return $ZC.storagesize($v, i))))
                      end))
    # `store_read_strategy` is *pinned* to `SequentialRead()` by default rather
    # than forwarded: `ZArray.jl` guards the `Channel`/`@async` concurrent
    # read/write path with `store_read_strategy(z.storage) isa SequentialRead`,
    # and that guard must fold to a constant or the concurrent path becomes
    # reachable (and it is not trim-clean). Every store that can legitimately be
    # a pool member (DirectoryStore, DictStore, ZipStore) is sequential anyway.
    # `read_strategy = forward` opts into the real (isa-chain) forwarding.
    if read_strategy === :sequential
        push!(out.args, :($ZC.store_read_strategy(::$SP) = $ZC.SequentialRead()))
    else
        push!(out.args, :(function $ZC.store_read_strategy(p::$SP)
                              s = p.s
                              $(_schain((v, M) -> :(return $ZC.store_read_strategy($v))))
                          end))
    end
    # A constant `show`: `show(io, s)` for a store without its own `show`
    # method pulls in the generic struct-display stack.
    push!(out.args, :(Base.show(io::IO, ::$SP) = print(io, $(string(SP)))))
    push!(out.args, :(function $ZC.has_configurable_missing_chunks(p::$SP)
                          s = p.s
                          $(_schain((v, M) -> :(return $ZC.has_configurable_missing_chunks($v))))
                      end))

    # ---- reader -----------------------------------------------------------
    ndtup = Expr(:curly, :Tuple, [:(Val{$n}) for n in nds]...)
    # A v3-only reader without a `codecs` pool has no `CodecPool_tag` to name in
    # the descriptor's first parameter; `Nothing` stands in for it (and is what
    # `ZarrTrimmable.codecpool` then reports).
    cpparam = emit_v2_codecs ? CP : :Nothing
    push!(out.args, :(const $RT = $ZT.ZarrReader{$cpparam,$SP,Tuple{$(dtypes...)},$ndtup}))

    arrtypes = Any[]
    if has2
        for T in dtypes, n in nds
            push!(arrtypes, :($ZC.ZArray{$T,$n,$SP,$ZC.MetadataV2{$T,$n,$CP,Nothing}}))
        end
    end
    if has3
        for T in dtypes, n in nds
            push!(arrtypes, :($ZC.ZArray{$T,$n,$SP,
                                         $ZC.MetadataV3{$T,$n,$PL,$ZC.ChunkKeyEncoding}}))
        end
    end
    push!(out.args, :(const $AU = Union{$(arrtypes...)}))

    # The message of the `zopen` that finds neither metadata document. Built at
    # macro-expansion time so the generated code carries a constant `String`.
    notfound = if has2 && has3
        "no Zarr array at the given path: neither .zarray (v2) nor zarr.json (v3) was found"
    elseif has2
        "no Zarr v2 array at the given path: .zarray not found"
    else
        "no Zarr v3 array at the given path: zarr.json not found"
    end

    # visitor leaves: one `zopen` call site per format, |dtypes| x |ndims|
    # specialisations each
    if has2
        push!(out.args, :(function $fleaf(f::F, ::Type{T}, ::Val{N}, store::$SP, sub::String) where {F,T,N}
            z = $ZC.zopen(T, Val(N), store, sub;
                          compressor = $CP)::$ZC.ZArray{T,N,$SP,$ZC.MetadataV2{T,N,$CP,Nothing}}
            return f(z)
        end))
        ndchain = _chain(Any[(:(n == $n), :(return $fleaf(f, T, Val($n), store, sub))) for n in nds],
                         :(throw(ArgumentError(string("rank ", string(n),
                              " is not in this reader's ndims pool")))))
        push!(out.args, :(function $fnd(f::F, ::Type{T}, n::Int, store::$SP, sub::String) where {F,T}
            $ndchain
        end))
    end
    if has3
        push!(out.args, :(function $fleaf3(f::F, ::Type{T}, ::Val{N}, store::$SP, sub::String) where {F,T,N}
            z = $ZC.zopen(T, Val(N), store, sub;
                          pipeline = $PL)::$ZC.ZArray{T,N,$SP,
                                                      $ZC.MetadataV3{T,N,$PL,$ZC.ChunkKeyEncoding}}
            return f(z)
        end))
        ndchain3 = _chain(Any[(:(n3 == $n), :(return $fleaf3(f, T, Val($n), store, sub))) for n in nds],
                          :(throw(ArgumentError(string("rank ", string(n3),
                               " is not in this reader's ndims pool")))))
        push!(out.args, :(function $fnd3(f::F, ::Type{T}, n3::Int, store::$SP, sub::String) where {F,T}
            $ndchain3
        end))
    end

    # visitor `zopen`: try `.zarray` first, then `zarr.json`, in whichever of
    # the two the reader's `zarr_format` pool asks for.
    vbody = Expr(:block)
    if has2
        dtchain = _chain(Any[(:(dt == $ZC.typestr($T)), :(return $fnd(f, $T, n, store, sub)))
                             for T in dtypes],
                         :(throw(ArgumentError(string("dtype \"", dt,
                              "\" is not in this reader's dtype pool")))))
        push!(vbody.args, quote
            b = store[sub, ".zarray"]
            if b !== nothing
                z = $ZC.parse_zarray(b)
                dt = z.dtype
                n = length(z.shape)
                $dtchain
            end
        end)
    end
    if has3
        dtchain3 = _chain(Any[(:(dt3 == $ZC.typestr3($T)), :(return $fnd3(f, $T, n3, store, sub)))
                              for T in dtypes],
                          :(throw(ArgumentError(string("dtype \"", dt3,
                               "\" is not in this reader's dtype pool")))))
        push!(vbody.args, quote
            b3 = store[sub, "zarr.json"]
            if b3 !== nothing
                z3 = $ZC.parse_zarrjson(b3)
                dt3 = z3.data_type
                n3 = length(z3.shape)
                $dtchain3
            end
        end)
    end
    push!(vbody.args, :(throw(ArgumentError($notfound))))
    push!(out.args, :(function $ZC.zopen(f::F, ::$RT, store::$SP, sub::String = "") where {F<:Function}
        $vbody
    end))
    push!(out.args, :(function $ZC.zopen(f::F, r::$RT, path::String, sub::String = "") where {F<:Function}
        return $ZC.zopen(f, r, $SP($pstore(path)), sub)
    end))

    # returning form. The limit counts the *members of the union*, so a reader
    # over both formats reaches it with half as many dtype x ndims pairs.
    nvariants = length(arrtypes)
    if nvariants <= 3
        rbody = Expr(:block)
        if has2
            push!(out.args, :(function $gleaf(::Type{T}, ::Val{N}, store::$SP, sub::String) where {T,N}
                return $ZC.zopen(T, Val(N), store, sub;
                                 compressor = $CP)::$ZC.ZArray{T,N,$SP,$ZC.MetadataV2{T,N,$CP,Nothing}}
            end))
            rndchain = _chain(Any[(:(n == $n), :(return $gleaf(T, Val($n), store, sub))) for n in nds],
                              :(throw(ArgumentError(string("rank ", string(n),
                                   " is not in this reader's ndims pool")))))
            push!(out.args, :(function $gnd(::Type{T}, n::Int, store::$SP, sub::String) where {T}
                $rndchain
            end))
            rdtchain = _chain(Any[(:(dt == $ZC.typestr($T)), :(return $gnd($T, n, store, sub)))
                                  for T in dtypes],
                              :(throw(ArgumentError(string("dtype \"", dt,
                                   "\" is not in this reader's dtype pool")))))
            push!(rbody.args, quote
                b = store[sub, ".zarray"]
                if b !== nothing
                    z = $ZC.parse_zarray(b)
                    dt = z.dtype
                    n = length(z.shape)
                    $rdtchain
                end
            end)
        end
        if has3
            push!(out.args, :(function $gleaf3(::Type{T}, ::Val{N}, store::$SP, sub::String) where {T,N}
                return $ZC.zopen(T, Val(N), store, sub;
                                 pipeline = $PL)::$ZC.ZArray{T,N,$SP,
                                                             $ZC.MetadataV3{T,N,$PL,$ZC.ChunkKeyEncoding}}
            end))
            rndchain3 = _chain(Any[(:(n3 == $n), :(return $gleaf3(T, Val($n), store, sub))) for n in nds],
                               :(throw(ArgumentError(string("rank ", string(n3),
                                    " is not in this reader's ndims pool")))))
            push!(out.args, :(function $gnd3(::Type{T}, n3::Int, store::$SP, sub::String) where {T}
                $rndchain3
            end))
            rdtchain3 = _chain(Any[(:(dt3 == $ZC.typestr3($T)), :(return $gnd3($T, n3, store, sub)))
                                   for T in dtypes],
                               :(throw(ArgumentError(string("dtype \"", dt3,
                                    "\" is not in this reader's dtype pool")))))
            push!(rbody.args, quote
                b3 = store[sub, "zarr.json"]
                if b3 !== nothing
                    z3 = $ZC.parse_zarrjson(b3)
                    dt3 = z3.data_type
                    n3 = length(z3.shape)
                    $rdtchain3
                end
            end)
        end
        push!(rbody.args, :(throw(ArgumentError($notfound))))
        push!(out.args, :(function $ZC.zopen(::$RT, store::$SP, sub::String = "")::$AU
            $rbody
        end))
        push!(out.args, :(function $ZC.zopen(r::$RT, path::String, sub::String = "")::$AU
            return $ZC.zopen(r, $SP($pstore(path)), sub)
        end))
    else
        msg = string("this reader has ", nvariants,
                     " dtype x ndims x zarr_format variants; a returning `zopen` would widen ",
                     "to `ZArray` (Base.Compiler.MAX_TYPEUNION_LENGTH = 3). Use the visitor ",
                     "form `zopen(f, reader, path)` instead.")
        push!(out.args, :(function $ZC.zopen(::$RT, store::$SP, sub::String = "")
            throw(ArgumentError($msg))
        end))
        push!(out.args, :(function $ZC.zopen(::$RT, path::String, sub::String = "")
            throw(ArgumentError($msg))
        end))
    end

    # ---- zcreate ----------------------------------------------------------
    # Creation is Zarr v2 only, and needs the v2 codec pool. A v3-only reader
    # that leaves `codecs` empty therefore gets no `zcreate` methods at all.
    if emit_v2_codecs
    defcodec = :($(codecs[1])())
    for n in nds
        dimargs = [:(dims[$i]) for i in 1:n]
        chunkargs = Expr(:tuple, [:(chunks[$i]) for i in 1:n]...)
        push!(out.args, :(function $ZC.zcreate(::$RT, store::$SP, ::Type{T}, dims::NTuple{$n,Int};
                                               codec = $defcodec,
                                               chunks::NTuple{$n,Int} = dims,
                                               fill_value::Union{Nothing,T} = nothing,
                                               sub::String = "") where {T}
            cp = $CP(codec)
            # two separate `zcreate` call sites: a `Union{Nothing,T}` flowing
            # into the `fill_value` keyword would make `MetadataV2`'s element
            # type value-dependent.
            if fill_value === nothing
                return $ZC.zcreate(T, store, $(dimargs...); path = sub, chunks = $chunkargs,
                                   compressor = cp, fill_value = nothing, filters = nothing
                    )::$ZC.ZArray{T,$n,$SP,$ZC.MetadataV2{T,$n,$CP,Nothing}}
            else
                fv = fill_value::T
                return $ZC.zcreate(T, store, $(dimargs...); path = sub, chunks = $chunkargs,
                                   compressor = cp, fill_value = fv, filters = nothing
                    )::$ZC.ZArray{T,$n,$SP,$ZC.MetadataV2{T,$n,$CP,Nothing}}
            end
        end))
        push!(out.args, :(function $ZC.zcreate(r::$RT, path::String, ::Type{T}, dims::NTuple{$n,Int};
                                               codec = $defcodec,
                                               chunks::NTuple{$n,Int} = dims,
                                               fill_value::Union{Nothing,T} = nothing,
                                               sub::String = "") where {T}
            return $ZC.zcreate(r, $SP($pstore(path)), T, dims;
                               codec = codec, chunks = chunks,
                               fill_value = fill_value, sub = sub)
        end))
    end
    end  # emit_v2_codecs

    # value of the macro: the reader instance
    push!(out.args, :($RT()))
    return out
end

"""
    @zarr_reader(dtypes = (...), ndims = (...), codecs = (...), stores = (...),
                 [zarr_format = (2,)], [v3_codecs = (...)],
                 [tag = Name], [path_store = DirectoryStore],
                 [read_strategy = sequential])

Generate a strongly typed Zarr reader over a closed set and return the reader
value. All pools are tuples of type names that must resolve in the calling
module at macro-expansion time.

```julia
reader = @zarr_reader(dtypes = (Float64, Int32), ndims = (2, 3),
                      codecs = (ZarrCore.NoCompressor, ZstdCompressor,
                                ZlibCompressor, BloscCompressor),
                      stores = (DirectoryStore,), tag = R1)

s = zopen(sum_all, reader, "data/f64_2d_zstd.zarr")          # visitor form
z = zcreate(reader, "out.zarr", Float64, (4, 6); chunks = (2, 3))
```

Emitted names (with `tag = R1`): `CodecPool_R1`, `StorePool_R1`,
`ReaderType_R1`, `ZArrayUnion_R1`, and for a reader that reads Zarr v3 also
`BBPool_R1` (the bytes->bytes codec pool) and `P_R1` (its pipeline type).
Without `tag` a counter-derived one is used.

The macro adds methods to `ZarrCore`'s own verbs; there is no separate API:

* `ZarrCore.zopen(f, reader, store_or_path[, sub])` -- visitor form. Always
  available; the concrete `ZArray` is only visible inside `f`.
* `ZarrCore.zopen(reader, store_or_path[, sub])` -- returns the array,
  annotated `::ZArrayUnion_R1`. Only emitted when the union has at most 3
  members (see `zarr_format` below); a wider reader gets a method that throws
  `ArgumentError`, because `tmerge` widens a merge of more than
  `Base.Compiler.MAX_TYPEUNION_LENGTH = 3` types to bare `ZArray`.
* `ZarrCore.zcreate(reader, store_or_path, T, dims::NTuple{N,Int};
  codec, chunks, fill_value, sub)` -- one method per rank in the `ndims` pool.
  **Creation is Zarr v2 only** and needs the `codecs` pool; a v3-only reader
  that leaves `codecs` empty gets no `zcreate` methods.

The codec and store pools cost **no** `ZArray` specialisations: they are
`Union`-typed fields of `CodecPool_R1`/`StorePool_R1`, and every interface
function is forwarded with an explicit `isa` chain, so a pool wider than the
compiler's union-splitting limit still resolves statically.

# Zarr v3: `zarr_format` and `v3_codecs`

`zarr_format` is a tuple literal drawn from `(2,)` (the default), `(3,)` and
`(2, 3)`; a bare integer (`zarr_format = 3`) is accepted too. The visitor
`zopen` then looks for `.zarray` (when 2 is in the pool) and for `zarr.json`
(when 3 is), in that order, and throws an `ArgumentError` naming the formats it
tried when it finds neither.

When 3 is in the pool, `v3_codecs` must be a non-empty tuple of
`V3Codec{:bytes,:bytes}` types; giving it without 3 in `zarr_format` is an
error. The members are listed explicitly rather than derived from `codecs` --
no cross-package v2-compressor-to-v3-codec mapping is involved.

```julia
reader = @zarr_reader(dtypes = (Float64,), ndims = (2,),
                      codecs = (ZarrCore.NoCompressor, ZstdCompressor),
                      stores = (DirectoryStore,),
                      zarr_format = (2, 3),
                      v3_codecs = (ZstdV3Codec, ZarrCore.CRC32cV3Codec),
                      tag = R3)
```

`BBPool_R3` is the v3 counterpart of `CodecPool_R3`: a `V3Codec{:bytes,:bytes}`
whose one field is the `Union` of the pool, forwarding `codec_encode`,
`codec_decode`, `name`, `JSON.lower` and `getCodec` with `isa` chains. The
pipeline the v3 leaves ask for is

    const P_R3 = ZarrCore.V3Pipeline{Tuple{}, ZarrCore.BytesCodec, Vector{BBPool_R3}}

so **any number of bytes->bytes codecs in any order** is accepted at no extra
cost, while the array->array stage is empty and the array->bytes codec is
always the plain `bytes` codec. A stored chain that does not fit -- a
`transpose`, `sharding_indexed` or `vlen-utf8` codec, or a bytes->bytes codec
outside the pool -- fails the open with an `ArgumentError`. Chunk key encodings
`default` and `v2` (any separator) work; `suffix` does not. See
[`ZarrCore.MetadataV3`](@ref) for the rest of the typed v3 path's limits
(regular chunk grid only, no `storage_transformers`, no `null` fill value).

The returning `zopen`'s 3-member limit counts the members of
`ZArrayUnion_R1`, so a `zarr_format = (2, 3)` reader reaches it with half as
many `dtypes` x `ndims` pairs as a v2-only one: the union gains one
`ZArray{T,N,StorePool_R1,MetadataV3{T,N,P_R1,ChunkKeyEncoding}}` per pair on
top of the `MetadataV2` member.

Keyword `read_strategy`:
`sequential` (default) pins `ZarrCore.store_read_strategy` to
`SequentialRead()`; `forward` forwards the members' real strategy, which makes
the `isa SequentialRead` guards in `ZArray.jl` non-constant and pulls the
`Channel`/`@async` path (not `--trim=safe`) into the program.

# Admitting a codec to a pool

A compressor type `C` may be a `codecs =` member once it has

1. `ZarrCore.codec_id(::Type{C})::String` — the numcodecs `"id"` it is stored
   under (`""` means "stored as `null`", i.e. a `NoCompressor`-alike), and
2. the four `ZarrCore` compressor methods the pool forwards to:
   `ZarrCore.getCompressor(::Type{C}, ::ZarrCore.CompressorJSON)`,
   `ZarrCore.zcompress!`, `ZarrCore.zuncompress!` and `JSON.lower`.

`ZarrZstd`, `ZarrZlib` and `ZarrBlosc` ship all five for their compressors, and
`ZarrCore.NoCompressor` has them too. Nothing else is needed — `ZarrTrimmable`
itself depends on no codec package.

# Admitting a codec to a `v3_codecs` pool

A v3 codec type `C` may be a `v3_codecs =` member once it is a
`ZarrCore.Codecs.V3Codecs.V3Codec{:bytes,:bytes}` and has

1. `V3Codecs.codec_name(::Type{C})::String` — the v3 wire name, without the
   optional `numcodecs.` prefix, and
2. `V3Codecs.getCodec(::Type{C}, ::ZarrCore.CodecJSON, ctx)`,
   `V3Codecs.codec_encode(::C, ::Vector{UInt8})`,
   `V3Codecs.codec_decode(::C, ::Vector{UInt8})` and `JSON.lower`
   (`V3Codecs.name(::C)` falls back to `codec_name`).

`ZstdV3Codec`, `GzipV3Codec`, `BloscV3Codec` and `ZarrCore.CRC32cV3Codec` ship
all of them.

!!! note "top level only"
    The macro defines `struct`s and `const`s, so it must be expanded at the top
    level of a module or script. Inside a function or a `@testset` body it fails
    with "unsupported `const` declaration on local variable".

!!! note "what a pool gives up"
    A `CodecPool` is not `NoCompressor`, so the uncompressed write fast path
    specialised on `MetadataV2{T,N,NoCompressor,Nothing}` is not taken; writes
    go through `zcompress!`. `ZarrCore.storefromstring` dispatches on the store
    *type* and cannot be mirrored by a `StorePool`. `ConsolidatedStore` and
    `CachingStore` must not be pool members: they wrap another store (the pool
    would become recursive) and override `getmetadata`/`getattrs` themselves,
    which the wrapper would silently bypass. The remote stores (`HTTPStore`,
    `S3Store`, `GCStore`) must not be members either — they are concurrent-read
    stores whose strategy the pool pins away, and their I/O is not trim-clean.

!!! note "what a pool costs"
    Measured on the juliac spike (`--trim=safe`, Julia 1.12):

    * **≈6 KB of binary per extra codec** in the `codecs` pool — the pool adds
      one `isa` branch per member per forwarded method, and no `ZArray`
      specialisations at all.
    * **≈175 KB of binary per extra `(dtype, ndims)` pair** — each one is a new
      `ZArray`/`MetadataV2` specialisation and its whole read/write stack. Widen
      `codecs` and `stores` freely; widen `dtypes` × `ndims` deliberately.
    * The `NoCompressor` **write fast path** (the
      `MetadataV2{T,N,NoCompressor,Nothing}` specialisation) is never taken
      through a pool, even when the pool's active member is a `NoCompressor`.
    * `ZarrCore.store_read_strategy` is **pinned** to `SequentialRead()` unless
      you pass `read_strategy = forward`, so the concurrent chunk path is
      unreachable (which is the point — it is not trim-clean).
    * `ConsolidatedStore`, `CachingStore`, `HTTPStore`, `S3Store` and `GCStore`
      must not be pool members (see above).
"""
macro zarr_reader(args...)
    dts = nothing; nds = nothing; cds = nothing; sts = nothing
    tag = nothing; pstore = nothing; rstrat = :sequential
    fmts = nothing; v3cds = nothing
    for a in args
        (a isa Expr && a.head === :(=)) ||
            error("@zarr_reader: expected `key = (...)` arguments, got ", a)
        k = a.args[1]
        k isa Symbol || error("@zarr_reader: bad keyword ", k)
        v = a.args[2]
        if k === :dtypes
            dts = _pool_items(v)
        elseif k === :ndims
            nds = _pool_items(v)
        elseif k === :codecs
            cds = _pool_items(v)
        elseif k === :stores
            sts = _pool_items(v)
        elseif k === :zarr_format
            fmts = _pool_items(v)
        elseif k === :v3_codecs
            v3cds = _pool_items(v)
        elseif k === :tag
            tag = v isa QuoteNode ? v.value : v
        elseif k === :path_store
            pstore = v
        elseif k === :read_strategy
            rstrat = v isa QuoteNode ? v.value : v
        else
            error("@zarr_reader: unknown keyword ", k)
        end
    end
    dts === nothing && error("@zarr_reader: `dtypes` is required")
    nds === nothing && error("@zarr_reader: `ndims` is required")
    sts === nothing && error("@zarr_reader: `stores` is required")
    if fmts === nothing
        formats = Int[2]
    else
        all(x -> x isa Integer, fmts) ||
            error("@zarr_reader: `zarr_format` must be integer literals")
        formats = Int[Int(x) for x in fmts]
    end
    # `codecs` stays required for a reader that reads Zarr v2 (the default); a
    # v3-only reader may leave it out entirely.
    cds === nothing && (2 in formats) && error("@zarr_reader: `codecs` is required")
    cds === nothing && (cds = Any[])
    v3cds === nothing && (v3cds = Any[])
    if tag === nothing
        _READER_COUNTER[] += 1
        tag = Symbol("A", _READER_COUNTER[])
    end
    tag isa Symbol || error("@zarr_reader: `tag` must be a plain name")
    rstrat isa Symbol ||
        error("@zarr_reader: `read_strategy` must be `sequential` or `forward`")
    ex = _zarr_reader_expr(dts, Any[n for n in nds], cds, sts, tag;
                           path_store = pstore, read_strategy = rstrat,
                           formats = formats, v3codecs = v3cds)
    return esc(ex)
end
