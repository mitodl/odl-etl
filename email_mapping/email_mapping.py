#!/usr/bin/env python

import os
import sys
from hashlib import sha256

import pandas as pd
import pyarrow as pa
import s3fs
import yaml
from botocore.exceptions import ClientError
from logbook import Logger, RotatingFileHandler
from pyarrow import parquet as pq

from sqlalchemy import create_engine
from sqlalchemy.exc import OperationalError

app_name = sys.argv[1]

RotatingFileHandler('{0}_etl.log'.format(app_name)).push_application()
log = Logger('{0}_etl'.format(app_name))

try:
    with open(os.path.join(os.path.dirname(__file__), '{0}_settings.yml'.format(app_name))) as etl_settings:
        settings = yaml.safe_load(etl_settings)
except FileNotFoundError:
    log.error(
        'No settings file present. Please add {0}_settings.yml and try again.'.format(app_name))
    sys.exit(1)

dest_file = '{0}_user_map.parquet'.format(app_name)

db_engine = create_engine(settings['db_url'])
try:
    db_conn = db_engine.connect()
except OperationalError:
    log.error(
        'Unable to connect to the source database.'
        ' Check the credentials and try again.'
    )
    sys.exit(1)

users_df = pd.read_sql(
    'select username, email from {0}'.format(
        settings['user_table'],
    ),
    db_conn,
)
users_df['email'] = users_df['email'].apply(
    lambda email: sha256(settings['hash_salt'].encode() + email.encode()).hexdigest(),
)

tbl = pa.Table.from_pandas(users_df, preserve_index=False)
pq.write_table(tbl, dest_file)

try:
    fs = s3fs.S3FileSystem(
        key=settings['aws_access_key_id'],
        secret=settings['aws_secret_access_key']
    )
    fs.put(
        dest_file,
        '{bucket}/{app_name}_user_map/{dest_file}'.format(
            bucket=settings['s3_bucket'],
            app_name=app_name,
            dest_file=dest_file,
        ),
    )
except ClientError:
    log.error('Unable to upload user map to S3')
    raise
