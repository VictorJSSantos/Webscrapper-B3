import json
import boto3
import os

# Inicializa os clientes do boto3
s3_client = boto3.client('s3')
glue_client = boto3.client('glue')

# Nome do bucket e pasta RAW
BUCKET_NAME = 's3-fiap-etl-250461282134'
RAW_FOLDER = 'raw/'
GLUE_JOB_NAME = 'JobETLDataB3'

def lambda_handler(event, context):
    try:
        # Lista todos os arquivos na pasta RAW do bucket S3
        response = s3_client.list_objects_v2(Bucket=BUCKET_NAME, Prefix=RAW_FOLDER)
        if 'Contents' not in response:
            return {
                'statusCode': 404,
                'body': json.dumps('Nenhum arquivo encontrado na pasta RAW.')
            }

        # Filtra apenas os arquivos .parquet
        parquet_files = [obj['Key'] for obj in response['Contents'] if obj['Key'].endswith('.parquet')]
        if not parquet_files:
            return {
                'statusCode': 404,
                'body': json.dumps('Nenhum arquivo .parquet encontrado na pasta RAW.')
            }

        print(f"Arquivos .parquet encontrados: {parquet_files}")

        # Inicia o job do AWS Glue
        glue_response = glue_client.start_job_run(JobName=GLUE_JOB_NAME)
        print(f"Glue job iniciado. Job ID: {glue_response['JobRunId']}")

        return {
            'statusCode': 200,
            'body': json.dumps({
                'message': 'Job iniciado com sucesso.',
                'parquet_files': parquet_files,
                'glue_job_id': glue_response['JobRunId']
            })
        }

    except Exception as e:
        print(f"Erro: {e}")
        return {
            'statusCode': 500,
            'body': json.dumps({'error': str(e)})
        }

