import boto3
import psycopg2
import csv
import io
from rapidfuzz import fuzz, process  # Biblioteca para fuzzy matching

# Inicializando clientes boto3 para SQS e S3
sqs_client = boto3.client('sqs')
s3_client = boto3.client('s3')

# Configurações da fila SQS e do bucket S3
SQS_QUEUE_URL = 'https://sqs.us-east-1.amazonaws.com/081383311767/atividade7'
S3_BUCKET_NAME = 'atividade7'
S3_FILE_KEY = 'resultado_final.csv'

# Configurações do banco de dados PostgreSQL
DB_HOST = "atividade7.cqun5qgzjgou.us-east-1.rds.amazonaws.com"
DB_USER = "postgres"
DB_PASSWORD = "postgres"
DB_NAME = "postgres"

# Limite de compatibilidade fuzzy (90%)
FUZZY_MATCH_THRESHOLD = 90

def lambda_handler(event, context):
    # Receber mensagens da fila SQS
    messages = receive_sqs_messages()
    if messages:
        # Conectar ao banco de dados PostgreSQL e enriquecer os dados
        enriched_data = enrich_data_from_db(messages)

        # Salvar o conteúdo final em um arquivo no S3
        save_to_s3(enriched_data)

    return {
        'statusCode': 200,
        'body': 'Mensagens processadas e enviadas para o S3 com sucesso.'
    }

def receive_sqs_messages():
    try:
        all_messages = []  # Lista para armazenar todas as mensagens recebidas

        while True:
            # Recebendo as mensagens da fila SQS
            response = sqs_client.receive_message(
                QueueUrl=SQS_QUEUE_URL,
                MaxNumberOfMessages=10,  # Ajuste o número de mensagens a ser recebido
                WaitTimeSeconds=5
            )
            messages = response.get('Messages', [])

            if not messages:
                # Se não há mais mensagens, saia do loop
                break
            
            all_messages.extend(messages)  # Adiciona as mensagens recebidas à lista

            # Processar cada mensagem
            #for message in messages:
                # Aqui você pode fazer o que precisar com cada mensagem
                #print(f'Processando mensagem: {message["MessageId"]}')
                
                # Lembre-se de excluir a mensagem após processá-la, se necessário
                #sqs_client.delete_message(
                #    QueueUrl=SQS_QUEUE_URL,
                #    ReceiptHandle=message['ReceiptHandle']
                #)
        
        print(f'Todas as mensagens recebidas: {len(all_messages)} mensagens.')
        return all_messages

    except Exception as e:
        print(f"Erro ao receber mensagens do SQS: {str(e)}")
        return []

def enrich_data_from_db(messages):
    try:
        # Conectando ao PostgreSQL
        conn = psycopg2.connect(
            host=DB_HOST,
            database=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD
        )
        cursor = conn.cursor()
        print("Conexão com o banco estabelecida!")
        
        enriched_data = []

        # Consultar todos os nomes de instituições financeiras no banco de dados
        cursor.execute("SELECT Nome FROM raw_enquadramentoinicia_v2")
        all_institutions = cursor.fetchall()
        institution_names = [row[0] for row in all_institutions]  # Extraindo os nomes
        
        # Enriquecer os dados de cada mensagem com dados do banco via fuzzy matching
        for message in messages[1:]:
            instituicao_financeira = message['Body'].split(';')[5] if len(message['Body'].split(';')) > 5 else None

            if instituicao_financeira:
                # Usar fuzzy matching para encontrar a melhor correspondência
                best_match = process.extractOne(instituicao_financeira, institution_names, scorer=fuzz.ratio)
                if best_match and best_match[1] >= FUZZY_MATCH_THRESHOLD:
                    # Obter informações da instituição correspondente
                    cursor.execute("""
                        SELECT * FROM raw_enquadramentoinicia_v2 WHERE Nome = %s
                    """, (best_match[0],))
                    db_result = cursor.fetchone()
                    
                    mensagem=message['Body'].split(';')
                    if db_result and len(db_result)>=3:
                        novos_valores = [db_result[0], str(db_result[1]), db_result[2]]
                        mensagem.extend(novos_valores)
                        enriched_data.append(';'.join(mensagem))

        # Fechar a conexão
        cursor.close()
        conn.close()
            
        return enriched_data

    except Exception as e:
        print(f"Erro ao conectar ao PostgreSQL: {str(e)}")
        return []
        
#PAREI AQUI
def save_to_s3(data):
    try:
        # Convertendo os dados para o formato CSV
        csv_data = "Ano;Trimestre;Categoria;Tipo;CNPJ IF;Instituição financeira;Índice;Quantidade de reclamações reguladas procedentes;Quantidade de reclamações reguladas - outras;Quantidade de reclamações não reguladas;Quantidade total de reclamações;Quantidade total de clientes  CCS e SCR;Quantidade de clientes  CCS;Quantidade de clientes  SCR;Segmento;CNPJ;Nome\n"
        for item in data:
           csv_data += f"{item}\n"
           print(csv_data)

        # Enviando o arquivo CSV para o S3
        s3_client.put_object(
            Bucket=S3_BUCKET_NAME,
            Key=f'Resultado/{S3_FILE_KEY}',
            Body=csv_data.encode('utf-8')
        )
        print(f"Arquivo salvo com sucesso no S3: {S3_FILE_KEY}")

    except Exception as e:
        print(f"Erro ao salvar arquivo no S3: {str(e)}")
