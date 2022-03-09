# Some examples on how to access public S3 datasets

With this package it is possible to access public datasets that are hosted remotely on
a s3-compatible cloud store. Here we provide examples on how to read data from commonly used datasets.

## Accessing data on Amazon S3

First we show how to access the zarr-demo bucket on AWS S3. We have to setup a
AWS configuration first, for options look at the documentation of
[AWS.jl](https://github.com/JuliaCloud/AWS.jl). If you don't have an
account, you can access the dataset without credentials as follows:

````@example aws
using Zarr, AWS
AWS.global_aws_config(AWSConfig(creds=nothing, region = "eu-west-2"))
````

Then we can directly open a zarr group stored on s3

````@example aws
z = zopen("s3://zarr-demo/store/foo/bar")
````

So we see that the store points to a zarr group with a single variable `baz`.

````@example aws
v = z["baz"]
````

The variable seems to contain an ASCIIString.

````@example aws
String(v[:])
````

## Accessing CMIP6 data on GCS

GCS is hosting a subset of the [CMIP6](https://pcmdi.llnl.gov/CMIP6/) climate model
ensemble runs. The data is stored in zarr format and accessible using this package.
There is a catalog that contains a table of all model runs available:

````julia
using DataFrames, CSV
overview = CSV.read(download("https://storage.googleapis.com/cmip6/cmip6-zarr-consolidated-stores.csv"),DataFrame)
````
````
138786×10 DataFrame. Omitted printing of 6 columns
│ Row    │ activity_id │ institution_id │ source_id  │ experiment_id │
│        │ String      │ String         │ String     │ String        │
├────────┼─────────────┼────────────────┼────────────┼───────────────┤
│ 1      │ AerChemMIP  │ BCC            │ BCC-ESM1   │ piClim-CH4    │
│ 2      │ AerChemMIP  │ BCC            │ BCC-ESM1   │ piClim-CH4    │
│ 3      │ AerChemMIP  │ BCC            │ BCC-ESM1   │ piClim-CH4    │
│ 4      │ AerChemMIP  │ BCC            │ BCC-ESM1   │ piClim-CH4    │
│ 5      │ AerChemMIP  │ BCC            │ BCC-ESM1   │ piClim-CH4    │
│ 6      │ AerChemMIP  │ BCC            │ BCC-ESM1   │ piClim-CH4    │
│ 7      │ AerChemMIP  │ BCC            │ BCC-ESM1   │ piClim-CH4    │
⋮
│ 138779 │ ScenarioMIP │ UA             │ MCM-UA-1-0 │ ssp585        │
│ 138780 │ ScenarioMIP │ UA             │ MCM-UA-1-0 │ ssp585        │
│ 138781 │ ScenarioMIP │ UA             │ MCM-UA-1-0 │ ssp585        │
│ 138782 │ ScenarioMIP │ UA             │ MCM-UA-1-0 │ ssp585        │
│ 138783 │ ScenarioMIP │ UA             │ MCM-UA-1-0 │ ssp585        │
│ 138784 │ ScenarioMIP │ UA             │ MCM-UA-1-0 │ ssp585        │
│ 138785 │ ScenarioMIP │ UA             │ MCM-UA-1-0 │ ssp585        │
│ 138786 │ ScenarioMIP │ UA             │ MCM-UA-1-0 │ ssp585        │
````

These columns contain the path to the store as well, so after some subsetting we can access
the member run we are interested in:

````julia
store = filter(overview) do row
  row.activity_id == "ScenarioMIP" && row.institution_id=="DKRZ" && row.variable_id=="tas" && row.experiment_id=="ssp585"
end
store.zstore[1]
````
````
"gs://cmip6/CMIP6/ScenarioMIP/DKRZ/MPI-ESM1-2-HR/ssp585/r1i1p1f1/3hr/tas/gn/v20190710/"
````

So we can access the dataset and read some data from it. Note that we use `consolidated=true` reduce
the overhead of repeatedly requesting many metadata files:

````julia
g = zopen(store.zstore[1], consolidated=true)
````

You can access the meta-information through `g.attrs` or for example read the first
time slice through

````julia
g["tas"][:,:,1]
````
````
384×192 reshape(::Array{Union{Missing, Float32},3}, 384, 192) with eltype Union{Missing, Float32}:
 244.27   245.276  245.186  245.419  …  252.782  252.852  252.672  252.667
 244.284  245.223  245.122  245.497     252.833  252.88   252.686  252.682
 244.309  245.139  245.003  245.422     252.85   252.895  252.704  252.663
 244.297  245.104  244.954  245.272     252.84   252.872  252.727  252.69
 244.352  245.055  244.835  245.182     252.858  252.895  252.739  252.69
 244.358  245.001  244.825  245.079  …  252.79   252.926  252.77   252.7  
 244.34   244.924  244.79   245.104     252.778  252.907  252.768  252.672
 244.348  244.87   244.737  245.112     252.756  252.928  252.755  252.712
 244.339  244.803  244.684  245.223     252.741  252.911  252.78   252.706
 244.383  244.723  244.649  245.005     252.729  252.842  252.78   252.719
   ⋮                                 ⋱                      ⋮             
 244.184  245.68   245.997  246.456  …  252.421  252.528  252.452  252.637
 244.186  245.649  245.907  246.313     252.518  252.546  252.469  252.643
 244.163  245.542  245.731  246.085     252.561  252.553  252.495  252.637
 244.227  245.491  245.68   246.178     252.643  252.596  252.534  252.678
 244.227  245.483  245.626  245.987     252.692  252.633  252.573  252.672
 244.253  245.442  245.497  245.975  …  252.756  252.682  252.577  252.631
 244.227  245.409  245.352  245.897     252.719  252.758  252.6    252.655
 244.296  245.356  245.231  245.774     252.735  252.809  252.612  252.659
 244.301  245.303  245.192  245.524     252.733  252.862  252.655  252.678
````

## Saving data to S3 using Minio.jl

In the examples above we only accessed data from several sources. Here we show 
how to store data on an own Minio server that we launch for testing purposes. First
we launch the Minio server:

````@example minio
using Minio
s = Minio.Server(tempname(), address="localhost:9005")
run(s, wait=false)
````

In the next step we configure AWS.jl to connect to our Minio instance by default. 
Afterwards we create an new bucket where we can store our data:

````@example minio
using AWS
cfg = MinioConfig("http://localhost:9005")
AWS.global_aws_config(cfg)
@service S3
S3.create_bucket("zarrdata")
````

Next we create a new zarr group in the just created bucket:

````@example minio
using Zarr
g = zgroup(S3Store("zarrdata"),"group_1")
````

and a new array inside the group and fill it with some data:

````@example minio
a = zcreate(Float32, g, "bar", 2,3,4, chunks=(1,2,2), attrs = Dict("att1"=>"one", "att2"=>2.5))
a[:,:,:] = reshape(1.0:24.0, (2,3,4))
````

Now we test if the data can be accessed

````@example minio
a2 = zopen("s3://zarrdata/group_1/array_1")
a2[2,2,1:4]
`````




