#!/usr/bin/env python

import os
from time import sleep

from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql import functions as F


# ─── CONFIGURACIÓN DEL ENTORNO ───────────────────────────────────────────────

# Le decimos a PySpark dónde está Java instalado en el sistema
os.environ["JAVA_HOME"] = "/usr/lib/jvm/java-21-openjdk-amd64"

# Dirección del broker Kafka
BOOTSTRAP_SERVERS_LOCAL = "localhost:9092"
BOOTSTRAP_SERVERS_LAB   = "localhost:9092"   # Grupo 2
KAFKA_BROKER = BOOTSTRAP_SERVERS_LAB

TOPIC = "purchases"

# Paquete Maven que conecta Spark con Kafka. Spark lo descarga automáticamente
# la primera vez. Importante que coincida la versión de Spark y Scala (2.13).
KAFKA_PACKAGE = "org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.1"

# Ficheros donde guardaremos los resultados de cada consulta
OUTPUT_FILE_1 = "salida1.txt"   # Consulta 1: agregación  (complete mode)
OUTPUT_FILE_2 = "salida2.txt"   # Consulta 2: filtrado    (append mode)

# Cuántas veces monitorizamos el stream raw antes de pasar a las consultas
MONITOR_ITERS = 5
SLEEP_SECS    = 15   # Segundos de espera entre iteraciones (y entre micro-batches)


# ─── INICIALIZACIÓN DE SPARK ─────────────────────────────────────────────────

conf = SparkConf()
# Indicamos el paquete Kafka para que Spark lo incluya en el classpath
conf.set("spark.jars.packages", KAFKA_PACKAGE)

spark = (SparkSession.builder
    .appName("StructuredStreamingKafka_Grupo2")
    .master("local[*]")   # Usamos todos los cores disponibles en local
    .config(conf=conf)
    .getOrCreate())

# Reducimos el nivel de log para no ahogarnos en mensajes de Spark
spark.sparkContext.setLogLevel("WARN")

print("=" * 60)
print(" Spark Structured Streaming - Grupo 2")
print(f" Broker : {KAFKA_BROKER}")
print(f" Topic  : {TOPIC}")
print("=" * 60)


# ─── LECTURA DEL STREAM DESDE KAFKA ──────────────────────────────────────────

# readStream crea un DataFrame "infinito": cada micro-batch trae nuevos mensajes.
# Kafka devuelve los datos como bytes, así que hacemos CAST a STRING.
input_data = (spark
    .readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BROKER)
    .option("subscribe", TOPIC)
    .option("startingOffsets", "earliest")   # Leemos desde el principio del topic
    .load()
    .selectExpr(
        "CAST(key   AS STRING) as usuario",   # La KEY   del mensaje (nombre de usuario)
        "CAST(value AS STRING) as producto",  # El VALUE del mensaje (nombre de producto)
    )
)


# ─── STREAM PRINCIPAL: raw_data ───────────────────────────────────────────────

# Guardamos el stream en memoria para poder consultarlo con Spark SQL.
# outputMode="append" solo añade las filas nuevas de cada micro-batch.
describe_query = (input_data.writeStream
    .queryName("raw_data")
    .format("memory")
    .outputMode("append")
    .start())

print("\n[raw_data] Stream iniciado. Monitorizando durante "
      f"{MONITOR_ITERS} iteraciones...\n")

# Cada SLEEP_SECS segundos imprimimos el estado actual de la tabla en memoria
for i in range(MONITOR_ITERS):
    print(f"--- Iteración {i + 1} / {MONITOR_ITERS} ---")
    spark.sql("SELECT * FROM raw_data").show(truncate=False)
    sleep(SLEEP_SECS)


# ─── CONSULTA 1: Conteo por producto (outputMode=complete) ───────────────────

print("\n[agg_data] Iniciando consulta de agregación (complete mode)...\n")

# Agrupamos todos los mensajes recibidos por nombre de producto y contamos
agg_data = (input_data
    .groupBy("producto")
    .agg(F.count("*").alias("total"))
)

# complete mode en cada micro-batch se reescribe la tabla ENTERA con los
# totales actualizados
agg_query = (agg_data.writeStream
    .queryName("agg_data")
    .format("memory")
    .outputMode("complete")
    .start())

sleep(SLEEP_SECS)   # Esperamos un micro-batch para que haya datos en memoria

# Consultamos la tabla y ordenamos de mayor a menor compras
result1 = spark.sql(
    "SELECT producto AS value, total "
    "FROM agg_data "
    "ORDER BY total DESC"
)
print("[Consulta 1] Compras por producto (todos los micro-batches acumulados):")
result1.show(truncate=False)

# Guardamos el resultado en un fichero de texto plano usando pandas
with open(OUTPUT_FILE_1, "w", encoding="utf-8") as f:
    f.write("CONSULTA 1 - Conteo de compras por producto (outputMode=complete)\n")
    f.write("=" * 60 + "\n")
    f.write(result1.toPandas().to_string(index=False))
    f.write("\n")

print(f"[Consulta 1] Resultado guardado en '{OUTPUT_FILE_1}'")


# ─── CONSULTA 2: Filtrado de productos específicos (outputMode=append) ────────

print("\n[filtered_data] Iniciando consulta de filtrado (append mode)...\n")

# Solo nos quedamos con los mensajes cuyo producto sea uno de estos tres
filtered_data = (input_data
    .filter(
        F.col("producto").isin(["book", "alarm clock", "t-shirts"])
    )
)

# append mode solo almacena las filas nuevas; válido aquí porque no hay
# agregación, cada fila filtrada es independiente y no cambia
filtered_query = (filtered_data.writeStream
    .queryName("filtered_data")
    .format("memory")
    .outputMode("append")
    .start())

sleep(SLEEP_SECS)

result2 = spark.sql("SELECT usuario, producto FROM filtered_data")
print("[Consulta 2] Mensajes filtrados (book, alarm clock, t-shirts):")
result2.show(truncate=False)

# Guardamos el resultado en un fichero de texto 
with open(OUTPUT_FILE_2, "w", encoding="utf-8") as f:
    f.write("CONSULTA 2 - Filtrado productos (outputMode=append)\n")
    f.write("=" * 60 + "\n")
    f.write(result2.toPandas().to_string(index=False))
    f.write("\n")

print(f"[Consulta 2] Resultado guardado en '{OUTPUT_FILE_2}'")


# ─── CIERRE LIMPIO ───────────────────────────────────────────────────────────

# Paramos todos los streams activos antes de cerrar la sesión de Spark.
print("\nCerrando streams y sesión de Spark...")
agg_query.stop()
filtered_query.stop()
describe_query.stop()
spark.stop()
print("Fin del programa.")
