import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from pyspark.context import SparkContext
from pyspark.sql import Window
from pyspark.sql.functions import avg, col, count, sum as _sum

# Setup do Glue
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Ler dados Brutos do S3
df = spark.read.option("recursiveFileLookup", "true").parquet("s3://tc2-bovespa-caio-2026/raw/")

# Garantir Tipos Corretos
df = df.withColumn("Date", col("Date").cast("string"))\
       .withColumn("Ticker", col("Ticker").cast("string"))


# Agrupamento numérico: média de Close e soma de Volume por Ticker
df_agg = df.groupBy("Ticker").agg(
    avg("Close").alias("avg_close"),
    _sum("Volume").alias("total_volume"),
    count("*").alias("dias_negociados")
) 

# Renomeando colunas
df = df.withColumnRenamed("Close", "preco_fechamento") \
       .withColumnRenamed("Volume", "volume_negociado")

# Antes de salvar refinado, garantir que partition keys são string
df = df.withColumn("Date", col("Date").cast("string")) \
       .withColumn("Ticker", col("Ticker").cast("string"))

# Média Movel de 7 dias do preço de fechamento
window_7d = Window.partitionBy("Ticker").orderBy("Date").rowsBetween(-6, 0)
df= df.withColumn("media_movel_7d", avg("preco_fechamento").over(window_7d))

# Salvar Agrupamento no s3 sumarizado/
df_agg_dynamic = DynamicFrame.fromDF(df_agg, glueContext, "df_agg_dynamic")
glueContext.write_dynamic_frame.from_options(
    frame=df_agg_dynamic,
    connection_type="s3",
    connection_options={"path": "s3://tc2-bovespa-caio-2026/sumarizado/"},
    format="parquet",
    transformation_ctx="sumarizado_sink"
)

# Salvar refinado no S3 e catalogar
df_dynamic = DynamicFrame.fromDF(df, glueContext, "df_dynamic")
glueContext.write_dynamic_frame.from_options(
    frame=df_dynamic,
    connection_type="s3",
    connection_options={
        "path": "s3://tc2-bovespa-caio-2026/refined/",
        "partitionKeys": ["Date", "Ticker"]
    },
    format="parquet",
    transformation_ctx="refined_sink"
)

job.commit()

