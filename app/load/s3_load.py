import boto3
import os
import sys

# Obtendo o nome do arquivo como argumento
if len(sys.argv) < 2:
    print("Erro: Nome do arquivo não fornecido.")
    sys.exit(1)

file_name = sys.argv[1]

# Creating session variables
AWS_ACCESS_KEY_ID = os.environ.get("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.environ.get("AWS_SECRET_ACCESS_KEY")
AWS_SESSION_TOKEN = os.environ.get("AWS_SESSION_TOKEN")
AWS_REGION = os.environ.get("AWS_REGION")

# Accessing session
session = boto3.Session(
    aws_access_key_id=AWS_ACCESS_KEY_ID,
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
    aws_session_token=AWS_SESSION_TOKEN,
    region_name=AWS_REGION
)

# Test it on a service
s3 = session.resource("s3")

# Parametrizações do envio do arquivo para o bucket S3
bucket = "fiap-etl-222987439421"
file_path = f"app/data/{file_name}"
key = f"raw/{file_name}"

with open(file_path, "rb") as data:
    s3.Bucket(bucket).put_object(Key=key, Body=data)
