#!/usr/bin/env python

from hashlib import sha256
from sqlalchemy import create_engine
from sqlalchemy.exc import OperationalError
from logbook import Logger, RotatingFileHandler
from botocore.exceptions import ClientError
import os
import yaml
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import s3fs
import sys

app_name = sys.argv[1]

RotatingFileHandler("{}_etl.log".format(app_name)).push_application()
log = Logger("{}_etl".format(app_name))

try:
    with open(os.path.join(os.path.dirname(__file__), "{}_settings.yml".format(app_name))) as etl_settings:
        settings = yaml.safe_load(etl_settings)[app_name]
except FileNotFoundError:
    log.error("No settings file present. Please add {}_settings.yml and try again.".format(app_name))
    exit(1)

dest_file = "{}_user_map.parquet".format(app_name)

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
