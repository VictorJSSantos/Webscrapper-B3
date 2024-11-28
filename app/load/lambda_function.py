import logging
import boto3


# Setting basic configs for Logging purposes
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# Creating the Client Object
glue = boto3.client("glue", region_name="us-east-1")


def lambda_handler(event, context):
    gluejobname = "Bovespa ELT"

    try:
        # Processing the event on S3 in order to obtain information like the bucket and key from the file.
        bucket = event["Records"][0]["s3"]["bucket"]["name"]
        key = event["Records"][0]["s3"]["object"]["key"]
        print(
            f"Arquivo {key} foi adicionado ao bucket {bucket}. Iniciando o Glue Job {gluejobname}."
        )

        # Initiate the Glue job
        runId = glue.start_job_run(JobName=gluejobname)
        print(f"Glue Job iniciado com ID: {runId['JobRunId']}")

        # Verify the state of the job
        status = glue.get_job_run(JobName=gluejobname, RunId=runId["JobRunId"])
        print("Job Status : ", status["JobRun"]["JobRunState"])
    except Exception as e:
        print("Erro ao iniciar o Glue Job:", e)
        raise e
