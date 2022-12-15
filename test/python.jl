###
### This test is to check against the reference zarr implementation in Python
### We save some data in Julia and python and test if it is still the same
### when read from teh other language 
###
@testset "Python zarr implementation" begin

using PyCall
import PyCall: @py_str
#If we are on conda, import zarr
pyimport_conda("zarr","zarr")

#Create some directories
proot = tempname()
mkpath(proot)
pjulia = joinpath(proot,"julia")
ppython = joinpath(proot,"python")

#First create an array in Julia and read with python zarr
groupattrs = Dict("String attribute"=>"One", "Int attribute"=>5, "Float attribute"=>10.5)
g = zgroup(pjulia,attrs=groupattrs)

# Test all supported data types and compressors
import Zarr: NoCompressor, BloscCompressor, ZlibCompressor, MaxLengthString
using Random: randstring
dtypes = (UInt8, UInt16, UInt32, UInt64,
    Int8, Int16, Int32, Int64,
    Float16, Float32, Float64,
    Complex{Float32}, Complex{Float64},
    Bool,MaxLengthString{10,UInt8},MaxLengthString{10,UInt32})
compressors = (
    "no"=>NoCompressor(),
    "blosc"=>BloscCompressor(cname="zstd"),
    "blosc_autoshuffle"=>BloscCompressor(cname="zstd",shuffle=-1),
    "blosc_noshuffle"=>BloscCompressor(cname="zstd",shuffle=0),
    "blosc_bitshuffle"=>BloscCompressor(cname="zstd",shuffle=2),
    "zlib"=>ZlibCompressor())
testarrays = Dict(t=>(t<:AbstractString) ? [randstring(maximum(i.I)) for i in CartesianIndices((1:10,1:6,1:2))] : rand(t,10,6,2) for t in dtypes)

for t in dtypes, co in compressors
    compstr, comp = co
    att = Dict("This is a nested attribute"=>Dict("a"=>5))
    a = zcreate(t, g,string("a",t,compstr),10,6,2,attrs=att, chunks = (5,2,2),compressor=comp)
    a[:,:,:] = testarrays[t]
end
# Test reading in python
py"""
import zarr
g = zarr.open_group($pjulia)
gatts = g.attrs
"""

#Test group attributes
@test py"gatts['String attribute']" == "One"
@test py"gatts['Int attribute']" == 5
@test py"gatts['Float attribute']" == 10.5


dtypesp = ("uint8","uint16","uint32","uint64",
    "int8","int16","int32","int64",
    "float16","float32","float64",
    "complex64", "complex128","bool","S10","U10")

#Test accessing arrays from python and reading data
for i=1:length(dtypes), co in compressors
    compstr,comp = co
    t = dtypes[i]
    tp = dtypesp[i]
    arname = string("a",t,compstr)
    py"""
    ar=g[$arname]
    """

    @test py"ar.attrs['This is a nested attribute']" == Dict("a"=>5)
    @test py"ar.dtype==$tp"
    @test py"ar.shape" == (2,6,10)
    if t<:MaxLengthString
      pyar = py"ar[:,:,:]"
      jar = [get(get(get(pyar,k-1),j-1),i-1) for i in 1:10, j in 1:6, k in 1:2]
      @test jar == testarrays[t]
    else
      @test py"ar[:,:,:]" == permutedims(testarrays[t],(3,2,1))
    end
end

## Now the other way around, we create a zarr array using the python lib and read back into julia
data = rand(Int32,2,6,10)
py"""
import numcodecs
g = zarr.group($ppython)
g.attrs["groupatt"] = "Hi"
z1 = g.create_dataset("a1", shape=(2,6,10),chunks=(1,2,3), dtype='i4')
z1[:,:,:]=$data
z1.attrs["test"]={"b": 6}
z2 = g.create_dataset("a2", shape=(5,),chunks=(5,), dtype='S1', compressor=numcodecs.Zlib())
z2[:]=[k for k in 'hallo']
zarr.consolidate_metadata($ppython)
"""

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
np = pyimport("numpy")

g_julia = zopen(p)
g_python = zarr.open(p)

for unit in ["Week", "Day", "Hour", "Minute", "Second", 
        "Millisecond"]
    @test_py np.datetime64(g_julia[unit][1] |> DateTime |> string) == get(getproperty(g_python,unit),0)
    @test_py np.datetime64(g_julia[unit][10] |> DateTime |> string) ==  get(getproperty(g_python,unit),9)
    @test_py np.datetime64(g_julia[unit][100] |> DateTime |> string) == get(getproperty(g_python,unit),99)
end



end
