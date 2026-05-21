# Práctica 1 — Pipelines de datos con Airflow
**SDPD2 | Grupo 2:** Javier Martín · Lara Montero · Mónica Silva · Celia López

---

## ¿De qué va esto?

En esta práctica montamos un pipeline ETL usando Apache Airflow. La idea es bastante sencilla: cogemos un CSV de reviews de Airbnb, lo limpiamos, le hacemos algunas transformaciones y al final lo mandamos a Kafka. Todo dividido en DAGs separados para que cada fase sea independiente.

---

## Archivos

```
├── 01_extracion_3.py        → DAG de extracción
├── 02_limpieza_2.py         → DAG de limpieza
├── 03_tranformaciones_2.py  → DAG de transformación
├── 04_kafka_2.py            → DAG de publicación en Kafka
└── config.toml              → rutas y configuración de Kafka
```

---

## Los DAGs

### 1. Extracción (`pipeline_extraccion_datos`)
Descarga el CSV desde GitHub y lo guarda en local. Las tareas son `extraer_datos` → `transformar_datos` → `cargar_resultado`. Sencillo, solo es el punto de entrada del pipeline.

### 2. Limpieza (`pipeline_limpieza_reviews`)
Aquí es donde más trabajo hay. Lee el CSV en bruto y hace varias cosas:
- Elimina duplicados (por ID de review y filas exactamente iguales)
- Quita filas con nulos
- Limpia espacios raros en nombres de usuario
- Normaliza los comentarios (quita saltos de línea, espacios dobles...)
- Convierte los IDs a string para que no salgan con decimales
- Parsea las fechas correctamente

El resultado se guarda en `reviews_clean.csv`.

### 3. Transformación (`pipeline_transformacion_reviews`)
Coge el archivo limpio y le añade columnas nuevas:
- `comments_clean`: comentarios en minúsculas y sin puntuación
- `comment_length`: longitud del comentario
- `word_count`: número de palabras
- `review_year` y `review_month`: extraídos de la columna de fecha

El resultado se guarda en `reviews_transformed.csv`.

### 4. Kafka (`pipeline_kafka_reviews`)
Lee el CSV transformado fila a fila y manda cada fila como un mensaje JSON al topic de Kafka. Cierra la conexión limpiamente con `producer.flush()` al final.

---

## Configuración

Todo lo que es rutas y parámetros está en `config.toml` para no tener que tocar el código si cambia algo:

```toml
[paths]
source_url       = "https://raw.githubusercontent.com/Lara-004/SDPD2/refs/heads/main/reviews.csv"
raw_data         = "reviews.csv"
clean_data       = "reviews_clean.csv"
transformed_data = "reviews_transformed.csv"

[kafka]
bootstrap_servers = "localhost:9092"
topic             = "reviews_clean"
```

---

## Cómo ejecutarlo

1. Meter los scripts en la carpeta `dags/` de Airflow
2. Asegurarse de que el `config.toml` está en la ruta que indica cada DAG
3. Lanzar los DAGs en orden desde la UI o con:

```bash
airflow dags trigger pipeline_extraccion_datos
airflow dags trigger pipeline_limpieza_reviews
airflow dags trigger pipeline_transformacion_reviews
airflow dags trigger pipeline_kafka_reviews
```

Kafka tiene que estar levantado en `localhost:9092` antes de lanzar el último DAG.

---

## Dependencias

```
apache-airflow
pandas
kafka-python
```

`tomllib` ya viene incluido en Python 3.11+, no hace falta instalarlo.

---

## Cosas a tener en cuenta

- Los DAGs tienen `schedule=None` así que hay que lanzarlos a mano, no se ejecutan solos.
- Para pasar datos entre tareas usamos diccionarios en vez de DataFrames directamente, porque Airflow no serializa bien objetos pandas entre tasks.
- Si el `config.toml` no está en la ruta esperada, el DAG falla con un `FileNotFoundError` bastante claro.
