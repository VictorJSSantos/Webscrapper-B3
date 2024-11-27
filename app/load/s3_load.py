import boto3
import os

# Creating session variables
AWS_ACCESS_KEY_ID = os.environ["AWS_ACCESS_KEY_ID"]
AWS_SECRET_ACCESS_KEY = os.environ["AWS_SECRET_ACCESS_KEY"]
AWS_SESSION_TOKEN = os.environ["AWS_SESSION_TOKEN"]
AWS_REGION = os.environ["AWS_REGION"]

# Accessing session
session = boto3.Session(
    aws_access_key_id=AWS_ACCESS_KEY_ID,
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
    aws_session_token=AWS_SESSION_TOKEN,
    region_name=AWS_REGION,
)


# Test it on a service
s3 = session.resource("s3")

# Adding file 'teste.parquet' into bucket 'fiap-tc-modulo-2-raw'
file_name = "27-11-24.parquet.gzip"
bucket = "fiap-tc-modulo-2-raw"
file_path = f"app/data/{file_name}"

with open(file_path, "rb") as data:
    s3.Bucket(bucket).put_object(Key=file_name, Body=data)
