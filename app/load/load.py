import boto3
from datetime import datetime
import logging
import os

# Creating session variables
AWS_ACCESS_KEY_ID = os.environ.get("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.environ.get("AWS_SECRET_ACCESS_KEY")
AWS_SESSION_TOKEN = os.environ.get("AWS_SESSION_TOKEN")
AWS_REGION = os.environ.get("AWS_REGION")
RAW_BUCKET = os.environ.get("RAW_BUCKET")

# Accessing session
session = boto3.Session(
    aws_access_key_id=AWS_ACCESS_KEY_ID,
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
    aws_session_token=AWS_SESSION_TOKEN,
    region_name=AWS_REGION,
)

# Configuração básica do logger
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)

# Logger para a aplicação
logger = logging.getLogger(__name__)


s3 = session.resource("s3")


def load_into_AWS_S3() -> None:

    # Adding file 'teste.parquet' into bucket
    date = datetime.now().strftime("%d-%m-%y")
    file_name = f"{date}.parquet.gzip"

    file_path = f"app/data/{file_name}"
    key = f"{file_name}"

    with open(file_path, "rb") as data:
        s3.Bucket(RAW_BUCKET).put_object(Key=key, Body=data)

    def add_file(file_name, file_path, bucket):
        """
        Adiciona um arquivo ao bucket S3 especificado.

        :param file_name: Nome do arquivo no bucket
        :param file_path: Caminho local do arquivo
        :param bucket: Nome do bucket S3
        """
        try:
            with open(file_path, "rb") as data:
                s3.Bucket(bucket).put_object(Key=file_name, Body=data)

            logger.info(f"O arquivo {file_name} foi adicionado ao bucket {bucket}")
        except Exception as e:
            logger.error(
                f"O arquivo {file_name} teve algum problema para ser enviado ao bucket {bucket}: {e}",
                exc_info=True,
            )

    # Testing if file exists to prevent from doubling spending on files that already exists (Lambda, ETLS runs)
    try:
        s3.Object(RAW_BUCKET, file_name).load()
        answer = (
            input("O arquivo já existe no bucket, deseja continuar? (y/n):")
            .strip()
            .lower()
        )
        if answer != "y":
            logger.info(f"Operação cancelada pelo usuário.")
        else:
            add_file(file_name, file_path, RAW_BUCKET)

    except Exception as e:
        if e.response["Error"]["Code"] == "404":
            logger.warning(
                f"Arquivo {file_name} não está presente no bucket {RAW_BUCKET} e iremos adicioná-lo"
            )
            add_file(file_name, file_path, RAW_BUCKET)
        else:
            logger.error(
                f"Erro ao tentar verificar o arquivo {file_name}: {e}", exc_info=True
            )
