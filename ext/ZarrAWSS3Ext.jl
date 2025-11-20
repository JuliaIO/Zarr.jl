module ZarrAWSS3Ext

import Zarr: Zarr, AbstractStore, cloud_list_objects, ConcurrentRead, storageregexlist
import AWSS3

include("s3store.jl")

end