import pandas as pd
import json
import psycopg2
from rapidfuzz import fuzz, process
from kafka import KafkaConsumer
from dotenv import load_dotenv
import os

load_dotenv()

# Configurações do Kafka
KAFKA_BROKER = 'localhost:9092'  # Altere para o endereço do seu broker
TOPIC_NAME = 'atividade8'         # Altere para o nome do seu tópico

# Configurações do PostgreSQL
DB_HOST = os.getenv('DB_HOST')
DB_NAME = os.getenv('DB_NAME')
DB_USER = os.getenv('DB_USER')
DB_PASSWORD = os.getenv('DB_PASSWORD')

# Limite de compatibilidade fuzzy (90%)
FUZZY_MATCH_THRESHOLD = 90

# Função para enriquecer os dados com consulta ao PostgreSQL
def enrich_data(messages):
    try:
        # Conectar ao PostgreSQL
        conn = psycopg2.connect(
            host=DB_HOST,
            database=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD
        )
        cursor = conn.cursor()

        # Consultar todos os nomes de instituições financeiras no banco de dados
        cursor.execute("SELECT banco_ds_nome FROM trusted_banco")
        all_institutions = cursor.fetchall()
        institution_names = [row[0] for row in all_institutions]  # Extraindo os nomes

        enriched = False  # Variável para verificar se houve enriquecimento

        # Verificar se o campo "Instituição financeira" está presente
        if "Instituição financeira" in messages:
            instituicao_financeira = messages["Instituição financeira"].replace(" (conglomerado)", "").strip()
            print(instituicao_financeira)

            if instituicao_financeira:
                # Usar fuzzy matching para encontrar a melhor correspondência
                best_match = process.extractOne(instituicao_financeira, institution_names, scorer=fuzz.ratio)
                if best_match and best_match[1] >= FUZZY_MATCH_THRESHOLD:
                    # Obter informações da instituição correspondente
                    cursor.execute("""
                        SELECT * FROM trusted_banco WHERE trusted_banco.banco_ds_nome = %s
                    """, (best_match[0],))
                    db_result = cursor.fetchone()

                    if db_result:
                        colunas = [desc[0] for desc in cursor.description]
                        messages.update(dict(zip(colunas, db_result)))  # Atualiza a mensagem com os dados do banco
                        print("Dados enriquecidos:", messages)
                        enriched = True  # Marcar como enriquecido

        # Fechar a conexão
        cursor.close()
        conn.close()

        return messages if enriched else None  # Retorna a mensagem apenas se houver enriquecimento

    except Exception as e:
        print(f"Erro ao conectar ao PostgreSQL: {str(e)}")
        return None  # Não retornar nada em caso de erro


def main():
    consumer = KafkaConsumer(
        TOPIC_NAME,
        bootstrap_servers=KAFKA_BROKER,
        value_deserializer=lambda x: json.loads(x.decode('utf-8')),
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        group_id='my-group'
    )

    file_name = 'registros_enriquecidos.csv'

    for message in consumer:
        record = message.value
        try:
            enriched_record = enrich_data(record)

            # Se houver enriquecimento, grava no CSV
            if enriched_record:
                df = pd.DataFrame([enriched_record])  # Transformar em DataFrame
                print(df)

                # Append ao CSV
                df.to_csv(file_name, mode='a', index=False, header=not os.path.exists(file_name))
                print("Registro adicionado ao CSV")
            else:
                print("Nenhum dado enriquecido. Registro ignorado.")

        except Exception as e:
            print(f"Erro ao incrementar o arquivo CSV: {str(e)}")


if __name__ == '__main__':
    main()