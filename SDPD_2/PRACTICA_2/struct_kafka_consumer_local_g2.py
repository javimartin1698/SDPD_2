#!/usr/bin/env python

import os
from time import sleep

from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql import functions as F


# Configuración del entorno
os.environ["JAVA_HOME"] = "/usr/lib/jvm/java-21-openjdk-amd64"

# Dirección del broker Kafka
BOOTSTRAP_SERVERS_LOCAL = "localhost:9092"
BOOTSTRAP_SERVERS_LAB   = "localhost:9092"   # Grupo 2
KAFKA_BROKER = BOOTSTRAP_SERVERS_LAB

TOPIC = "purchases"

# Conector Spark-Kafka para PySpark 4.1.1 (Scala 2.13)
KAFKA_PACKAGE = "org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.1"

# Archivos de salida para las dos consultas
OUTPUT_FILE_1 = "salida1.txt"   # Consulta 1: aggregation (complete mode)
OUTPUT_FILE_2 = "salida2.txt"   # Consulta 2: filter      (append mode)

# Número de iteraciones de monitorización del stream principal
MONITOR_ITERS = 5
SLEEP_SECS    = 15


# Inicialización de Spark
conf = SparkConf()
conf.set("spark.jars.packages", KAFKA_PACKAGE)

spark = (SparkSession.builder
    .appName("StructuredStreamingKafka_Grupo2")
    .master("local[*]")
    .config(conf=conf)
    .getOrCreate())

spark.sparkContext.setLogLevel("WARN")

print("=" * 60)
print(" Spark Structured Streaming - Grupo 2")
print(f" Broker : {KAFKA_BROKER}")
print(f" Topic  : {TOPIC}")
print("=" * 60)


# Lectura del stream desde Kafka (mantenemos clave y valor)
input_data = (spark
    .readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BROKER)
    .option("subscribe", TOPIC)
    .option("startingOffsets", "earliest")
    .load()
    .selectExpr(
        "CAST(key   AS STRING) as usuario",
        "CAST(value AS STRING) as producto",
    )
)


# Stream principal: raw_data (outputMode=append)
describe_query = (input_data.writeStream
    .queryName("raw_data")
    .format("memory")
    .outputMode("append")
    .start())

print("\n[raw_data] Stream iniciado. Monitorizando durante "
      f"{MONITOR_ITERS} iteraciones...\n")

for i in range(MONITOR_ITERS):
    print(f"--- Iteración {i + 1} / {MONITOR_ITERS} ---")
    spark.sql("SELECT * FROM raw_data").show(truncate=False)
    sleep(SLEEP_SECS)


# CONSULTA 1: Conteo por producto (outputMode=complete)
print("\n[agg_data] Iniciando consulta de agregación (complete mode)...\n")

agg_data = (input_data
    .groupBy("producto")                  # Agrupamos por nombre de producto
    .agg(F.count("*").alias("total"))     # Contamos cuántas veces aparece cada uno
)

agg_query = (agg_data.writeStream
    .queryName("agg_data")
    .format("memory")
    .outputMode("complete")               # Resultado completo en cada micro-batch
    .start())

sleep(SLEEP_SECS)                         # Esperamos a que el micro-batch procese datos

# Renombramos "producto" -> "value" para mantener el formato pedido
result1 = spark.sql(
    "SELECT producto AS value, total "
    "FROM agg_data "
    "ORDER BY total DESC"
)
print("[Consulta 1] Compras por producto (todos los micro-batches acumulados):")
result1.show(truncate=False)

# Volcamos el resultado a salida1.txt
with open(OUTPUT_FILE_1, "w", encoding="utf-8") as f:
    f.write("CONSULTA 1 - Conteo de compras por producto (outputMode=complete)\n")
    f.write("=" * 60 + "\n")
    f.write(result1.toPandas().to_string(index=False))
    f.write("\n")

print(f"[Consulta 1] Resultado guardado en '{OUTPUT_FILE_1}'")


# CONSULTA 2: Filtrado de productos específicos (outputMode=append)
print("\n[filtered_data] Iniciando consulta de filtrado (append mode)...\n")

filtered_data = (input_data
    .filter(
        F.col("producto").isin(["book", "alarm clock", "t-shirts"])
    )
)

filtered_query = (filtered_data.writeStream
    .queryName("filtered_data")
    .format("memory")
    .outputMode("append")                 # Solo las filas nuevas de cada micro-batch
    .start())

sleep(SLEEP_SECS)

result2 = spark.sql("SELECT usuario, producto FROM filtered_data")
print("[Consulta 2] Mensajes filtrados (book, alarm clock, t-shirts):")
result2.show(truncate=False)

# Volcamos el resultado a salida2.txt
with open(OUTPUT_FILE_2, "w", encoding="utf-8") as f:
    f.write("CONSULTA 2 - Filtrado productos (outputMode=append)\n")
    f.write("=" * 60 + "\n")
    f.write(result2.toPandas().to_string(index=False))
    f.write("\n")

print(f"[Consulta 2] Resultado guardado en '{OUTPUT_FILE_2}'")

print("\nCerrando streams y sesión de Spark...")
agg_query.stop()
filtered_query.stop()
describe_query.stop()
spark.stop()
print("Fin del programa.")
