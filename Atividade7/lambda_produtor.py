import boto3
import csv
import os

# Definindo clientes
s3_client = boto3.client('s3')
sqs_client = boto3.client('sqs')

# Defina a URL da SQS
SQS_QUEUE_URL = 'https://sqs.us-east-1.amazonaws.com/081383311767/atividade7'

#Função da lambda que recebe o evento
def lambda_handler(event, context):
    for record in event['Records']:
        #definindo o bucket e o arquivo através da inserção no s3
        s3_bucket = record['s3']['bucket']['name']
        s3_key = record['s3']['object']['key']
        print(s3_bucket)
        print(s3_key)
        s3_response = s3_client.get_object(Bucket=s3_bucket, Key=s3_key)
        
        #realizando leitura do arquivo inserido
        csv_content = s3_response['Body'].read().decode('ISO-8859-1').splitlines()
        #processando o arquivo
        csv_reader = csv.reader(csv_content)
        #extraindo linha por linha do csv e criando mensagem pra fila
        for row in csv_reader:
            message_body = ','.join(row)  
            send_to_sqs(message_body)
    return {
        'statusCode': 200,
        'body': (f'O Arquivo {str(s3_key)} foi processado e enviado para a fila SQS com sucesso.')
    }

def send_to_sqs(message_body):
    try:
        #envia a mensagem do csv para o SQS
        response = sqs_client.send_message(
            QueueUrl=SQS_QUEUE_URL,
            MessageBody=message_body
        )
        print(f'Mensagem enviada para fila SQS: {response["MessageId"]}')
    except Exception as e:
        print(f'Erro ao enviar a mensagem para fila SQS: {str(e)}')