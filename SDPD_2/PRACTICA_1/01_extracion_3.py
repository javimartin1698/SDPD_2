import requests
import pathlib
import tomllib

from datetime import datetime
from airflow import DAG
from airflow.decorators import dag, task

@task
def extraer_datos():
    # Busco el archivo de configuración para saber de dónde bajar los datos
    try:
        with open("/home/vboxuser/airflow/dags/config.toml", "rb") as f:
            config = tomllib.load(f)
    except FileNotFoundError:
        raise FileNotFoundError("Ojo, falta el config.toml en la ruta")

    # Saco la URL y la ruta de guardado del diccionario del config
    url = config["paths"]["source_url"]
    destino = config["paths"]["raw_data"]

    print(f"Descargando de: {url}")

    # Hago la petición y fuerzo error si la web no responde bien
    response = requests.get(url, timeout=20)
    response.raise_for_status()

    # Si la carpeta no existe, la creo antes de guardar nada
    ruta_salida = pathlib.Path(destino)
    ruta_salida.parent.mkdir(parents=True, exist_ok=True)

    # Guardo el contenido en el disco
    with open(ruta_salida, "wb") as f:
        f.write(response.content)

    return str(ruta_salida) # Devuelvo la ruta para que la use la siguiente función


@task
def transformar_datos(ruta_fichero):
    # Cogemos el archivo que acabamos de descargar
    ruta_original = pathlib.Path(ruta_fichero)
    
    # Le cambio el nombre para que acabe en "_clean" (ej: datos_clean.csv)
    ruta_transformada = ruta_original.with_name(ruta_original.stem + "_clean" + ruta_original.suffix)

    print(f"Ruta para el archivo limpio: {ruta_transformada}")
    return str(ruta_transformada)


@task
def cargar_resultado(ruta_final):
    # Aquí es donde conectaríamos con la base de datos o Kafka
    print(f"Todo listo. El archivo final está en: {ruta_final}")


# Configuración general del proceso (DAG)
@dag(
    dag_id="pipeline_extraccion_datos",
    start_date=datetime(2026, 3, 20),
    schedule=None,      # Lo lanzamos a mano
    catchup=False,      # No rellena ejecuciones pasadas
    tags=["practica", "etl"]
)
def mi_pipeline():
    # Aquí definimos el orden: extraer -> transformar -> cargar
    ruta_raw = extraer_datos()
    ruta_clean = transformar_datos(ruta_raw)
    cargar_resultado(ruta_clean)

# Ejecutamos la función para que Airflow detecte el DAG
dag = mi_pipeline()