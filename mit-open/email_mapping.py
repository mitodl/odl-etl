#!/usr/bin/env python

from hashlib import sha256
from sqlalchemy import create_engine
from sqlalchemy.exc import OperationalError
from logbook import Logger, RotatingFileHandler
from botocore.exceptions import ClientError
import yaml
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import s3fs

RotatingFileHandler("mit_open_etl.log").push_application()
log = Logger("mit_open_etl")

try:
    with open("etl_settings.yml") as etl_settings:
        settings = yaml.safe_load(etl_settings)["mit-open"]
except FileNotFoundError:
    log.error("No settings file present. Please add etl_settings.yml" " and try again.")
    exit(1)

dest_file = "user_map.parquet"

db_engine = create_engine(settings["db_url"])
try:
    db_conn = db_engine.connect()
except OperationalError as e:
    log.error(
        "Unable to connect to the source database."
        " Check the credentials and try again."
    )
    exit(1)

users_df = pd.read_sql("select username, email from auth_user", db_conn)
users_df["email"] = users_df["email"].apply(
    lambda x: sha256(settings["hash_salt"].encode() + x.encode()).hexdigest()
)

tbl = pa.Table.from_pandas(users_df, preserve_index=False)
pq.write_table(tbl, dest_file)

try:
    fs = s3fs.S3FileSystem(
        key=settings["aws_access_key_id"], secret=settings["aws_secret_access_key"]
    )
    fs.put(dest_file, '{bucket}/user_map/{dest_file}'.format(
        bucket=settings["s3_bucket"], dest_file=dest_file))
except ClientError:
    log.error("Unable to upload user map to S3")
    raise
