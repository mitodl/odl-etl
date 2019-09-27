import boto3

s3 = boto3.client('s3')
prefixes = s3.list_objects(Bucket='mitodl-data-lake', Prefix='mailgun/', Delimiter='/')['CommonPrefixes']

glue_targets = [{'Path': f's3://mitodl-data-lake/{prefix["Prefix"]}', 'Exclusions': []} for prefix in prefixes]

glue = boto3.client('glue')
glue.update_crawler(Name='mailgun-data', Targets={'S3Targets': glue_targets})
