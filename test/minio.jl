platform_str = if Sys.islinux()
    "linux"
elseif Sys.isapple()
    "darwin"
elseif Sys.iswindows()
    "windows"
else
    error("Unsupported OS")
end
minio = download("https://dl.minio.io/server/minio/release/$(platform_str)-amd64/minio")
chmod(minio,0o500)

newdir = mktempdir()
datadir = mkdir(joinpath(newdir,"data"))
ENV["MINIO_SECRET_KEY"] = "12345678"
ENV["MINIO_ACCESS_KEY"] = "username"
ENV["MINIO_REGION_NAME"] = "thuringia"
serv = @async run(`$minio server $datadir`)

