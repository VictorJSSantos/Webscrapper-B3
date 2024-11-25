import boto3

print("Loading function")

glue = boto3.client("glue", region_name="us-east-1")


def lambda_handler(event, context):
    gluejobname = "GLUE_JOB_NAME"

    try:
        # Processa o evento do S3 para obter informações do arquivo
        bucket = event["Records"][0]["s3"]["bucket"]["name"]
        key = event["Records"][0]["s3"]["object"]["key"]
        print(
            f"Arquivo {key} foi adicionado ao bucket {bucket}. Iniciando o Glue Job {gluejobname}."
        )

        # Inicia o Glue Job
        runId = glue.start_job_run(JobName=gluejobname)
        print(f"Glue Job iniciado com ID: {runId['JobRunId']}")

        # Opcional: Verifica o status inicial do Glue Job
        status = glue.get_job_run(JobName=gluejobname, RunId=runId["JobRunId"])
        print("Job Status : ", status["JobRun"]["JobRunState"])
    except Exception as e:
        print("Erro ao iniciar o Glue Job:", e)
        raise e
