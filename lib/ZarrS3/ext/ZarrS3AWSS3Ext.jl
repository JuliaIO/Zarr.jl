module ZarrS3AWSS3Ext

# Extend ZarrCore operations for ZarrS3.S3Store.
import ZarrS3
import ZarrS3: S3Store

import ZarrCore
import ZarrCore: cloud_list_objects, ConcurrentRead, concurrent_io_tasks, zopen

using AWSS3: AWSS3, s3_put, s3_get, s3_delete, s3_list_objects, s3_exists, S3Path, get_config

include("s3store.jl")

end
