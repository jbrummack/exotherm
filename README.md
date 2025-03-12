<p align="left"><img src="assets/logo2.png" alt="exotherm logo" height="120px"></p>

# ORM for KV Stores


## Supported backends
- FoundationDB (for distributed deployment)
- Fjall (planned; for mobile deployment)


## Installation (FoundationDB)
- Install FoundationDB (either run a container or use the MacOS installer)
```
# use a container without AVX for Apple Silicon (denoted by an even version number like 7.3.62)
docker run -p 4500:4500 --name fdb -it --rm -d foundationdb/foundationdb:7.3.62
docker exec fdb fdbcli --exec "configure new single memory"
```

## Installation (Fjall)
- Everything is already bundled, no hassle required ;)
