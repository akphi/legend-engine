# Instruction

- Run `IceboxSparkWithNessieCatalog.java`
- Copy the `INIT_SQL` and `S3_URL` info (example below) into a file, e.g. `~/iceberg-metadata`

```
INIT_SQL=INSTALL iceberg;LOAD iceberg;INSTALL httpfs;LOAD httpfs;SET s3_region='us-east-1';SET s3_url_style='path';SET s3_use_ssl = false;SET s3_endpoint='localhost:61369';SET s3_access_key_id='admin';SET s3_secret_access_key='password';
S3_URL=s3://warehouse/wh/nyc/taxis_ea06b885-a700-4a17-be3a-ee153ad9ec9d/metadata/00001-27c80166-4dd8-4893-b03b-13a350e316f9.metadata.json
```

- Open REPL (with Iceberg extension) `IcebergReplClient.java`
- Load the Iceberg S3 metadata

```shell
load_iceberg_s3 ~/iceberg-metadata mytable
datacube table mytable # now this should open your table in DataCube
```
