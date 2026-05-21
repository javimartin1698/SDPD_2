import pandas as pd
import tomllib
from airflow.sdk import dag, task
from datetime import datetime


@dag(
    dag_id="pipeline_limpieza_reviews",
    start_date=datetime(2026, 4, 1),
    schedule=None,
    catchup=False,
    tags=["reviews", "limpieza", "sdpd2"]
)
def pipeline_limpieza_reviews():
    """
    DAG para limpiar el CSV de reviews y guardarlo ya pulido.
    """

    @task
    def cargar_config():
        # Cargamos las rutas desde el toml para no ponerlas a mano en el código
        try:
            with open("config.toml", "rb") as f:
                config = tomllib.load(f)
            return config
        except FileNotFoundError:
            print("Error: no se encuentra el archivo config.toml")
            return None

    @task
    def limpieza_df(config):
        # Si no cargó el config arriba, aquí no podemos hacer nada
        if config is None:
            return None

        # Pillamos la ruta del archivo sucio
        input_file = config["paths"]["raw_data"]
        print(f"Leyendo archivo para limpieza: {input_file}")

        # Cargamos el CSV (el low_memory es para que no se queje con archivos grandes)
        df = pd.read_csv(input_file, low_memory=False)

        # Fuera duplicados basándonos en el ID de la review
        if "id" in df.columns:
            df = df.drop_duplicates(subset=["id"], keep="first")

        # Limpieza rápida: quitamos nulos y filas que sean exactamente iguales
        df = df.dropna()
        df = df.drop_duplicates()

        # Quitamos espacios raros en los nombres de los usuarios
        if "reviewer_name" in df.columns:
            df["reviewer_name"] = df["reviewer_name"].astype(str).str.strip()

        # Limpiamos los comentarios: quitamos saltos de línea y espacios dobles para que todo vaya en una línea
        if "comments" in df.columns:
            df["comments"] = (
                df["comments"]
                .astype(str)
                .str.replace(r"[\r\n]+", " ", regex=True)
                .str.replace(r"\s+", " ", regex=True)
                .str.strip()
            )

        # Pasamos los IDs a entero y luego a string para que no salgan con decimales (.0)
        id_columns = ["listing_id", "id", "reviewer_id"]
        for col in id_columns:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0).astype(int).astype(str)

        # Formateamos la fecha correctamente
        if "date" in df.columns:
            df["date"] = pd.to_datetime(df["date"], errors="coerce")

        print("Limpieza completada con éxito.")

        # Pasamos el DF a diccionario porque Airflow no suele llevarse bien pasando objetos DataFrame pesados
        return df.to_dict(orient="records")

    @task
    def guardar_limpieza_df(data, config):
        # Chequeo de seguridad por si falla algo antes
        if data is None or config is None:
            print("No hay datos o configuración para guardar.")
            return None

        # Convertimos otra vez a DataFrame para guardar el CSV
        df = pd.DataFrame(data)

        # Ruta de destino sacada del config
        output_path = config["paths"]["clean_data"]

        # Guardamos el resultado final sin el índice de pandas
        df.to_csv(output_path, index=False)

        print(f"Archivo limpio guardado en: {output_path}")
        return output_path

    # Aquí montamos el flujo: Config -> Limpiar -> Guardar
    config = cargar_config()
    data_limpia = limpieza_df(config)
    guardar_limpieza_df(data_limpia, config)


# Lanzamos la función para registrar el DAG
pipeline_limpieza_reviews()