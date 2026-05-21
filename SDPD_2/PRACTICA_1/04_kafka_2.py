import json
import pathlib
import csv
import tomllib
from airflow.sdk import dag, task
from datetime import datetime
from kafka import KafkaProducer


@dag(
    dag_id="pipeline_kafka_reviews",
    start_date=datetime(2026, 4, 1),
    schedule=None,
    catchup=False,
    tags=["reviews", "kafka", "sdpd2"]
)
def pipeline_kafka_reviews():
    """
    Este DAG lee el CSV final y manda los datos a un topic de Kafka.
    """

    @task
    def cargar_config():
        # Sacamos los datos de conexión y rutas del config.toml
        try:
            with open("config.toml", "rb") as f:
                config = tomllib.load(f)
            return config
        except FileNotFoundError:
            print("Error: No se encuentra el archivo config.toml")
            return None

    @task
    def publish_to_kafka(config):
        if config is None:
            print("No se pudo cargar la configuración.")
            return None

        # Pillamos los datos de Kafka y la ruta del archivo transformado
        input_file = pathlib.Path(config["paths"]["transformed_data"])
        topic_name = config["kafka"]["topic"]
        bootstrap_servers = config["kafka"]["bootstrap_servers"]

        print(f"Conectando a Kafka en: {bootstrap_servers}...")

        # Un check rápido para no intentar leer un archivo que no existe
        if not input_file.exists():
            print(f"Error: El archivo {input_file} no existe.")
            return None

        try:
            # Configuramos el productor para que convierta los dicts a JSON automáticamente
            producer = KafkaProducer(
                bootstrap_servers=bootstrap_servers,
                value_serializer=lambda v: json.dumps(v).encode("utf-8")
            )

            # Abrimos el CSV y lo recorremos fila por fila
            with input_file.open(mode="r", encoding="utf-8", newline="") as infile:
                reader = csv.DictReader(infile)
                count = 0

                for row in reader:
                    # Mandamos cada fila como un mensaje individual al topic
                    producer.send(topic_name, value=row)
                    count += 1

            # El flush es clave para asegurar que no se quede nada en el buffer antes de cerrar
            producer.flush()

            print(f"Éxito: Se han enviado {count} mensajes al topic '{topic_name}'.")
            return count

        except Exception as e:
            # Si Kafka está caído o hay algún lío de red, saltará por aquí
            print(f"Error al conectar o enviar a Kafka: {e}")
            return None

        finally:
            # Cerramos la conexión si llegamos a crear el productor
            if "producer" in locals():
                producer.close()

    # Ejecución simple: primero config y luego publicar
    config = cargar_config()
    publish_to_kafka(config)


# Registro del DAG en Airflow
pipeline_kafka_reviews()