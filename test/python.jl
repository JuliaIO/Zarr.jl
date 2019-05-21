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
import Zarr: NoCompressor, BloscCompressor
dtypes = (UInt8, UInt16, UInt32, UInt64,
    Int8, Int16, Int32, Int64,
    Float16, Float32, Float64,
    Complex{Float32}, Complex{Float64},
    Bool)
compressors = (NoCompressor(), BloscCompressor(cname="zstd"))
testarrays = Dict(t=>rand(t,10,6,2) for t in dtypes)

for t in dtypes, comp in compressors
    compstr = isa(comp,NoCompressor) ? "no" : "blosc"
    att = Dict("This is a nested attribute"=>Dict("a"=>5))
    a = zcreate(t, g,string("a",t,compstr),10,6,2,attrs=att, chunks = (5,2,2))
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
    "complex64", "complex128","bool")

#Test accessing arrays from python and reading data
for i=1:length(dtypes), comp in compressors
    t = dtypes[i]
    tp = dtypesp[i]
    compstr = isa(comp,NoCompressor) ? "no" : "blosc"
    arname = string("a",t,compstr)
    py"""
    ar=g[$arname]
    """
    @test py"ar.attrs['This is a nested attribute']" == Dict("a"=>5)
    @test py"ar.dtype==$tp"
    @test py"ar.shape" == (2,6,10)
    @test py"ar[:,:,:]" == permutedims(testarrays[t],(3,2,1))
end

## Now the other way around, we create a zarr array using the python lib and read back into julia
data = rand(Int32,2,6,10)
py"""
g = zarr.group($ppython)
z1 = g.create_dataset("a1", shape=(2,6,10),chunks=(1,2,3), dtype='i4')
z1[:,:,:]=$data
z1.attrs["test"]={"b": 6}
z2 = g.create_dataset("a2", shape=(5,),chunks=(5,), dtype='S1')
z2[:]=[k for k in 'hallo']
"""

#Open in Julia
g = zopen(ppython)
@test g isa Zarr.ZGroup
a1 = g["a1"]
@test a1 isa ZArray
@test a1[:,:,:]==permutedims(data,(3,2,1))
@test a1.attrs["test"]==Dict("b"=>6)
# Test reading the string array
@test String(g["a2"][:])=="hallo"
end
