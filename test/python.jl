###
### This test is to check against the reference zarr implementation in Python
### We save some data in Julia and python and test if it is still the same
### when read from the other language 
###
@testset "Python zarr implementation" begin

import Mmap
using PythonCall
#If we are on conda, import zarr
zarr = pyimport("zarr")

#Create some directories
proot = tempname()
mkpath(proot)
pjulia = joinpath(proot,"julia")
ppython = joinpath(proot,"python")

#First create an array in Julia and read with python zarr
groupattrs = Dict("String attribute"=>"One", "Int attribute"=>5, "Float attribute"=>10.5)
g = zgroup(pjulia,attrs=groupattrs)

# Test all supported data types and compressors
import Zarr: NoCompressor, BloscCompressor, ZlibCompressor, MaxLengthString, 
       Fletcher32Filter, FixedScaleOffsetFilter, ShuffleFilter, QuantizeFilter, DeltaFilter
using Random: randstring
numeric_dtypes = (UInt8, UInt16, UInt32, UInt64,
    Int8, Int16, Int32, Int64,
    Float16, Float32, Float64,
    Complex{Float32}, Complex{Float64},
    Bool,)
dtypes = (numeric_dtypes...,
    MaxLengthString{10,UInt8},MaxLengthString{10,UInt32},
    String)
dtypesp = ("uint8","uint16","uint32","uint64",
    "int8","int16","int32","int64",
    "float16","float32","float64",
    "complex64", "complex128","bool","S10","U10", "O")
compressors = (
    "no"=>NoCompressor(),
    "blosc"=>BloscCompressor(cname="zstd"),
    "blosc_autoshuffle"=>BloscCompressor(cname="zstd",shuffle=-1),
    "blosc_noshuffle"=>BloscCompressor(cname="zstd",shuffle=0),
    "blosc_bitshuffle"=>BloscCompressor(cname="zstd",shuffle=2),
    "zlib"=>ZlibCompressor())
filters = (
    "fletcher32"=>Fletcher32Filter(),
    "scale_offset"=>FixedScaleOffsetFilter(offset=1000, scale=10^6, T=Float64, Tenc=Int32),
    "shuffle"=>ShuffleFilter(elementsize=4),
    "quantize"=>QuantizeFilter{Float64,Float32}(digits=5),
    "delta"=>DeltaFilter{Int32}()
)
testarrays = Dict(t=>(t<:AbstractString) ? [randstring(maximum(i.I)) for i in CartesianIndices((1:10,1:6,1:2))] : rand(t,10,6,2) for t in dtypes)
testzerodimarrays = Dict(t=>(t<:AbstractString) ? randstring(10) : rand(t) for t in dtypes)

# Test arrays with compressors
for t in dtypes, co in compressors
    compstr, comp = co
    att = Dict("This is a nested attribute"=>Dict("a"=>5))
    a = zcreate(t, g,string("a",t,compstr),10,6,2,attrs=att, chunks = (5,2,2),compressor=comp)
    a[:,:,:] = testarrays[t]

    a = zcreate(t, g,string("azerodim",t,compstr), compressor=comp)
    a[] = testzerodimarrays[t]
end

# Test arrays with filters
for (filterstr, filter) in filters
    t = eltype(filter) == Any ? Float64 : eltype(filter)
    att = Dict("Filter test attribute"=>Dict("b"=>6))
    a = zcreate(t, g,string("filter_",filterstr),10,6,2,attrs=att, chunks = (5,2,2),filters=[filter])
    testdata = rand(t,10,6,2)
    a[:,:,:] = testdata
    
    # Test zero-dimensional array
    a = zcreate(t, g,string("filter_zerodim_",filterstr), filters=[filter])
    testzerodim = rand(t)
    a[] = testzerodim
end

#Also save as zip file.
open(pjulia*".zip";write=true) do io
    Zarr.writezip(io, g)
end

@testset "reading in julia" begin
    g = zopen(pjulia)
    #Test group attributes
    @test g.attrs == groupattrs
    for (t, co) in Iterators.product(dtypes, compressors)
        compstr,comp = co
        arname = string("a",t,compstr)
        ar = g[arname]
        @test ar.attrs == Dict("This is a nested attribute"=>Dict("a"=>5))
        @test ar == testarrays[t]
    end
end

# Test reading in python
for julia_path in (pjulia, pjulia*".zip")
    g = zarr.open_group(julia_path)
    gatts = pyconvert(Any, g.attrs)

    #Test group attributes
    @test gatts["String attribute"] == "One"
    @test gatts["Int attribute"] == 5
    @test gatts["Float attribute"] == 10.5

    #Test accessing arrays from python and reading data
    for i=1:length(dtypes), co in compressors
        compstr,comp = co
        t = dtypes[i]
        tp = dtypesp[i]
        arname = string("a",t,compstr)
        ar=g[arname]

        @test pyconvert(Any, ar.attrs["This is a nested attribute"]) == Dict("a"=>5)
        @test pyeq(Bool, ar.dtype, tp)
        @test pyconvert(Tuple, ar.shape) == (2,6,10)
        if t<:MaxLengthString || t<:String
            jar = [
                if tp == "S10"
                    pyconvert(String, ar[k, j, i].decode())
                else
                    pyconvert(String, ar[k, j, i])
                end
                for i in 0:9, j in 0:5, k in 0:1
            ]
            @test jar == testarrays[t]
        else
            @test PyArray(ar[pybuiltins.Ellipsis]) == permutedims(testarrays[t],(3,2,1))
        end
    end

    # Test reading filtered arrays from python
    for (filterstr, filter) in filters
        t = eltype(filter) == Any ? Float64 : eltype(filter)
        arname = string("filter_",filterstr)
        local ar
        try
            ar=g[arname]
        catch e
            @error "Error loading group with filter $filterstr" exception=(e,catch_backtrace())
            @test false # test failed.
        end
        
        @test pyconvert(Any, ar.attrs["Filter test attribute"]) == Dict("b"=>6)
        @test pyconvert(Tuple, ar.shape) == (2,6,10)
        
        # Test zero-dimensional filtered array
        arname = string("filter_zerodim_",filterstr) 
        ar_zero=g[arname]
        @test pyconvert(Tuple, ar_zero.shape) == ()
    end

    for i=1:length(dtypes), co in compressors
        compstr,comp = co
        t = dtypes[i]
        tp = dtypesp[i]
        if t == UInt64
            continue
            # need to exclude UInt64:
            # need explicit conversion because of https://github.com/JuliaPy/PyCall.jl/issues/744
            # but explicit conversion uses PyLong_AsLongLongAndOverflow, which converts everything
            # to a signed 64-bit integer, which can error out if the UInt64 is too large.
            # Adding an overload to PyCall for unsigned ints doesn't work with NumPy scalars because
            # they are not subtypes of integer: https://stackoverflow.com/a/58816671
        end

        arname = string("azerodim",t,compstr)
        ar=g[arname]

        @test pyeq(Bool, ar.dtype, tp)
        @test pyconvert(Tuple, ar.shape) == ()
        if t<:MaxLengthString || t<:String
            local x = if tp == "S10"
                pyconvert(String, ar[()].decode())
            else
                pyconvert(String, ar[()])
            end
            @test x == testzerodimarrays[t]
        else
            @test pyconvert(Any, ar[()])[] == testzerodimarrays[t]
        end
    end
    g.store.close()
end

## Now the other way around, we create a zarr array using the python lib and read back into julia
data = rand(Int32,2,6,10)

numpy = pyimport("numpy")
numcodecs = pyimport("numcodecs")
g = zarr.group(ppython)
g.attrs["groupatt"] = "Hi"
z1 = g.create_dataset("a1", shape=(2,6,10),chunks=(1,2,3), dtype="i4")
z1[pybuiltins.Ellipsis] = numpy.array(data)
z1.attrs["test"] = pydict(Dict("b"=>6))
z2 = g.create_dataset("a2", shape=(5,),chunks=(5,), dtype="S1", compressor=numcodecs.Zlib())
z2[pybuiltins.Ellipsis] = pylist([k for k in "hallo"])
z3 = g.create_dataset("a3", shape=(2,), dtype=pybuiltins.str)
z3[pybuiltins.Ellipsis]=numpy.asarray(["test1", "test234"], dtype="O")
zarr.consolidate_metadata(ppython)

#Open in Julia
g = zopen(ppython)
@test g isa Zarr.ZGroup
@test g.attrs["groupatt"] == "Hi"
a1 = g["a1"]
@test a1 isa ZArray
@test a1[:,:,:]==permutedims(data,(3,2,1))
@test a1.attrs["test"]==Dict("b"=>6)
# Test reading the string array
@test String(g["a2"][:])=="hallo"
@test g["a3"] == ["test1", "test234"]

# And test for consolidated metadata
# Delete files so we make sure they are not accessed
rm(joinpath(ppython,".zattrs"))
rm(joinpath(ppython,"a1",".zattrs"))
rm(joinpath(ppython,"a1",".zarray"))
rm(joinpath(ppython,"a2",".zarray"))
g = zopen(ppython, "w", consolidated=true)
@test g isa Zarr.ZGroup
@test g.attrs["groupatt"] == "Hi"
a1 = g["a1"]
@test a1 isa ZArray
@test a1[:,:,:]==permutedims(data,(3,2,1))
@test a1.attrs["test"]==Dict("b"=>6)
@test storagesize(a1) == 960
@test sort(Zarr.subkeys(a1.storage,"a1"))[1:5] == ["0.0.0","0.0.1","0.0.2","0.0.3","0.1.0"]
a1[:,1,1] = 1:10
@test a1[:,1,1] == 1:10
# Test reading the string array
@test String(g["a2"][:])=="hallo"


# Test zip file can be read
ppythonzip = ppython*".zip"
store = zarr.ZipStore(ppythonzip, mode="w")
g = zarr.group(store=store)
g.attrs["groupatt"] = "Hi"
z1 = g.create_dataset("a1", shape=(2,6,10),chunks=(1,2,3), dtype="i4")
z1[pybuiltins.Ellipsis] = numpy.array(data)
z1.attrs["test"] = pydict(Dict("b"=>6))
z2 = g.create_dataset("a2", shape=(5,),chunks=(5,), dtype="S1", compressor=numcodecs.Zlib())
z2[pybuiltins.Ellipsis] = pylist([k for k in "hallo"])
z3 = g.create_dataset("a3", shape=(2,), dtype=pybuiltins.str)
z3[pybuiltins.Ellipsis] = numpy.asarray(["test1", "test234"], dtype="O")
store.close()

g = zopen(Zarr.ZipStore(Mmap.mmap(ppythonzip)))
@test g isa Zarr.ZGroup
@test g.attrs["groupatt"] == "Hi"
a1 = g["a1"]
@test a1 isa ZArray
@test a1[:,:,:]==permutedims(data,(3,2,1))
@test a1.attrs["test"]==Dict("b"=>6)
# Test reading the string array
@test String(g["a2"][:])=="hallo"
@test g["a3"] == ["test1", "test234"]

end

@testset "Python datetime types" begin
using Dates, Test, Zarr
vd = Date(1970,1,1):Day(1):Date(1970,6,30) |> collect
vt = DateTime(1970,1,1):Second(1):DateTime(1970,1,1,2,0,0)|> collect
ad = ZArray(vd)
at = ZArray(vt)
@test eltype(ad)==Zarr.DateTime64{Day} 
@test eltype(at)==Zarr.DateTime64{Millisecond}
@test DateTime.(at[:]) == vt[:]
@test Date.(ad[:]) == vd[:]

p = tempname()
g = zgroup(p)
for pt in [Week, Day, Hour, Minute, Second, 
        Millisecond]
    
    if pt <: DatePeriod
        vd = range(Date(1970,1,1),step = pt(1), length=100)
        a = zcreate(Zarr.DateTime64{pt},g,string(pt),100)
        a[:] = vd
    else
        vd = range(DateTime(1970,1,1),step = pt(1), length=100)
        a = zcreate(Zarr.DateTime64{pt},g,string(pt),100)
        a[:] = vd
    end
end

zarr = pyimport("zarr")
numpy = pyimport("numpy")
g_julia = zopen(p)
g_python = zarr.open(p)

for unit in ["Week", "Day", "Hour", "Minute", "Second", 
        "Millisecond"]
    for i in [0, 9, 99]
        @test pyeq(Bool, numpy.datetime64(g_julia[unit][i+1] |> DateTime |> string), g_python[unit][i])
    end
end

end
