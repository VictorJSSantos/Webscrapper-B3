def lambda_handler(event, context):
    if "Records" not in event:
        print("O evento não contém a chave 'Records'.")
        return {"statusCode": 400, "body": "Evento inválido. 'Records' não encontrado."}

    try:
        bucket = event["Records"][0]["s3"]["bucket"]["name"]
        key = event["Records"][0]["s3"]["object"]["key"]
        print(f"Arquivo {key} foi adicionado ao bucket {bucket}.")

        # Seu código para iniciar o Glue Job

    except KeyError as e:
        print(f"Erro na estrutura do evento: {e}")
        return {"statusCode": 400, "body": f"Erro no evento recebido: {e}"}
