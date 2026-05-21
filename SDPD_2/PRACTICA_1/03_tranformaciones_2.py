import pandas as pd
import tomllib
from airflow.sdk import dag, task
from datetime import datetime


@dag(
    dag_id="pipeline_transformacion_reviews",
    start_date=datetime(2026, 4, 1),
    schedule=None,
    catchup=False,
    tags=["reviews", "transformacion", "sdpd2"]
)
def pipeline_transformacion_reviews():
    """
    Este DAG coge el archivo que ya limpiamos y le añade métricas nuevas
    antes de guardar el resultado final.
    """

    @task
    def cargar_config():
        # Lo de siempre: abrimos el toml para pillar las rutas de los archivos
        try:
            with open("config.toml", "rb") as f:
                config = tomllib.load(f)
            return config
        except FileNotFoundError:
            print("Error: No se encuentra el archivo config.toml")
            return None

    @task
    def get_transformed_df(config):
        if config is None:
            return None

        # Cargamos el CSV que salió del paso de limpieza
        input_file = config["paths"]["clean_data"]
        print(f"Leyendo archivo para transformación: {input_file}")
        df = pd.read_csv(input_file)

        # Si hay comentarios, los maquillamos un poco para análisis posterior
        if "comments" in df.columns:
            # Quitamos los nulos para que no casque al intentar pasarlo a string
            df["comments"] = df["comments"].fillna("").astype(str)

            # Creamos una columna nueva normalizada: todo a minúsculas y sin puntuación
            df["comments_clean"] = (
                df["comments"]
                .str.lower()
                .str.replace(r"\s+", " ", regex=True) # Fuera espacios múltiples
                .str.replace(r"[^\w\s]", "", regex=True) # Quitamos puntos, comas, etc.
                .str.strip()
            )

            # Calculamos métricas básicas de texto (longitud y número de palabras)
            df["comment_length"] = df["comments_clean"].str.len()
            df["word_count"] = df["comments_clean"].str.split().str.len()

        # Si tenemos la fecha, sacamos el año y el mes por separado
        if "date" in df.columns:
            df["date"] = pd.to_datetime(df["date"], errors="coerce")
            df["review_year"] = df["date"].dt.year
            df["review_month"] = df["date"].dt.month

            # Rellenamos con 0 si alguna fecha venía mal para que no queden como floats (nan)
            df["review_year"] = df["review_year"].fillna(0).astype(int)
            df["review_month"] = df["review_month"].fillna(0).astype(int)

        print("Transformación completada con éxito.")

        # Devolvemos el diccionario para que Airflow lo pase a la siguiente tarea sin dramas
        return df.to_dict(orient="records")

    @task
    def save_transformed_df(data, config):
        # Control de errores básico por si el paso anterior falló
        if data is None or config is None:
            print("No hay datos o configuración para guardar.")
            return None

        # Pasamos el diccionario de nuevo a DataFrame de Pandas
        df = pd.DataFrame(data)

        # Sacamos la ruta definitiva del archivo transformado
        output_path = config["paths"]["transformed_data"]

        # Exportamos a CSV
        df.to_csv(output_path, index=False)

        print(f"Archivo transformado guardado en: {output_path}")
        return output_path

    # Definimos el flujo de trabajo
    config = cargar_config()
    data_transformada = get_transformed_df(config)
    save_transformed_df(data_transformada, config)


# Iniciamos el pipeline
pipeline_transformacion_reviews()