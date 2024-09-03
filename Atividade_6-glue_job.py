import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import col, regexp_replace, trim
from awsglue.dynamicframe import DynamicFrame

# Configuração do Glue
sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init('raw_to_trusted', {})

# Caminhos no S3
s3_raw_path = 's3://datainge/RAW/'
s3_trusted_path = 's3://datainge/TRUSTED/'
s3_delivery_path = 's3://datainge/DELIVERY/'

# Processamento dos arquivos "BANKS"
banks_df = spark.read.option("header", True).option("sep", ";").csv(f"{s3_raw_path}BANKS/*.csv")\
    .union(spark.read.option("header", True).option("sep", "\t").csv(f"{s3_raw_path}BANKS/*.tsv"))

banks_df = banks_df.withColumn('Nome', regexp_replace(col('Nome'), ' - PRUDENCIAL', ''))\
                   .withColumn('CNPJ', col('CNPJ').cast('string'))

banks_df.write.mode('overwrite').parquet(f"{s3_trusted_path}BANKS/")

# Processamento dos arquivos "EMPLOYERS"
employers_df = spark.read.option("header", True).option("sep", "|").csv(f"{s3_raw_path}EMPLOYERS/*.csv")
employers_df.write.mode('overwrite').parquet(f"{s3_trusted_path}EMPLOYERS/")

# Processamento dos arquivos "REVIEWS"
reviews_df = spark.read.option("header", True).option("sep", ";").csv(f"{s3_raw_path}REVIEWS/*.csv")

reviews_df = reviews_df.withColumnRenamed('CNPJ IF', 'CNPJ')\
                       .dropna(subset=['CNPJ'])\
                       .withColumn('CNPJ', col('CNPJ').cast('string'))

reviews_df.write.mode('overwrite').parquet(f"{s3_trusted_path}REVIEWS/")

# Leitura dos arquivos Parquet da camada "TRUSTED" e combinação dos DataFrames
banks_df = spark.read.parquet(f"{s3_trusted_path}BANKS/")
employers_df = spark.read.parquet(f"{s3_trusted_path}EMPLOYERS/")
reviews_df = spark.read.parquet(f"{s3_trusted_path}REVIEWS/")

# Merge do DataFrame "BANKS" com o DataFrame "EMPLOYERS"
df_banks_employers = banks_df.join(employers_df, on='Nome', how='inner')

# Merge do DataFrame "REVIEWS" com o DataFrame "BANKS_EMPLOYERS"
df_final = reviews_df.join(df_banks_employers, on='CNPJ', how='inner').dropDuplicates()

# Limpeza da coluna 'Quantidade de clientes CCS'
df_final = df_final.withColumn('Quantidade de clientes � CCS', trim(col('Quantidade de clientes � CCS')))

# Escrita do DataFrame final em Parquet e CSV na camada "DELIVERY"
df_final.write.mode('overwrite').parquet(s3_delivery_path)
#df_final.write.mode('overwrite').format('csv').option('header', True).save(s3_delivery_path)
"""
# Convertendo o DataFrame final para DynamicFrame
dyf_final = DynamicFrame.fromDF(df_final, glueContext, "dyf_final")
# Declaracao da connection criada
glue_connection_name = "Jdbc connection"
# Salvando o DataFrame no banco de dados PostgreSQL usando a connection do Glue
glueContext.write_dynamic_frame.from_options(
    frame=dyf_final,
    connection_type="postgresql",
    connection_options={
        "connectionName": glue_connection_name,
        "dbtable": "banks_info",
        "database": "spark-ingestion"
    }
)
"""
job.commit()