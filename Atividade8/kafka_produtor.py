import pandas as pd
from kafka import KafkaProducer
import json
import os

# Configurações do Kafka
KAFKA_BROKER = 'localhost:9092'  # Altere para o endereço do seu broker
TOPIC_NAME = 'atividade8'         # Altere para o nome do seu tópico

# Função para enviar mensagens para o Kafka
def send_to_kafka(data):
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BROKER,
        value_serializer=lambda x: json.dumps(x, ensure_ascii=False).encode('utf-8')
    )

    # Enviando cada registro para o Kafka
    for record in data:
        producer.send(TOPIC_NAME, value=record)

    producer.flush()
    producer.close()

# Função para ler todos os arquivos CSV de um diretório e enviar os dados
def read_csv_and_send(directory):
    for filename in os.listdir(directory):
        if filename.endswith('.csv'):
            csv_file_path = os.path.join(directory, filename)
            print(f'Lendo o arquivo: {csv_file_path}')

            # Lendo o arquivo CSV
            df = pd.read_csv(csv_file_path, delimiter=';',encoding='utf-8')
            # Convertendo DataFrame para uma lista de dicionários
            records = df.to_dict(orient='records')

            # Enviando os registros para o Kafka
            send_to_kafka(records)
            print(f'{len(records)} registros enviados para o Kafka do arquivo {filename}.')

if __name__ == '__main__':
    directory_path = '/home/maria_abichabki/dados/reclamacoes'  # Altere para o caminho do seu diretório
    read_csv_and_send(directory_path)