module ZarrAWSS3Ext

import Zarr
import Zarr:
    S3Store,
    AbstractStore,
    cloud_list_objects,
    ConcurrentRead,
    storageregexlist,
    concurrent_io_tasks,
    zopen

using AWSS3: AWSS3, s3_put, s3_get, s3_delete, s3_list_objects, s3_exists, S3Path, get_config

include("s3store.jl")

end