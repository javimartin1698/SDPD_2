# Práctica 2 — Spark Structured Streaming con Kafka
**SDPD2 | Grupo 2:** Javier Martín · Lara Montero · Mónica Silva · Celia López

---

## ¿De qué va esto?

En esta práctica conectamos Spark con Kafka para procesar mensajes en tiempo real. Básicamente: un productor manda mensajes de compras aleatorias al topic `purchases`, y Spark los lee como un stream y hace consultas sobre ellos. Se implementan dos consultas: un conteo por producto y un filtrado por tipo de artículo.

---

## Archivos

```
├── kafka-producer-confluent_g2.py      → publica mensajes en Kafka
├── kafka-consumer-confluent_g2.py      → consumidor básico para verificar
├── struct_kafka_consumer_local_g2.py   → script principal con Spark
├── salida1.txt                          → resultado de la consulta de agregación
└── salida2.txt                          → resultado de la consulta de filtrado
```

---

## Cómo ejecutarlo

Kafka tiene que estar corriendo en `localhost:9092` antes de empezar.

```bash
# Terminal 1 — activar entorno y lanzar el productor
source ~/kafka_env/bin/activate
python3 kafka-producer-confluent_g2.py

# Terminal 2 — verificar que llegan los mensajes (opcional)
python3 kafka-consumer-confluent_g2.py

# Terminal 3 — ejecutar Spark (con el entorno pyspark-411)
python3 struct_kafka_consumer_local_g2.py
```

---

## Qué hace cada script

**Productor** (`kafka-producer-confluent_g2.py`): genera 10 mensajes aleatorios combinando usuarios y productos de dos listas predefinidas. Usa `acks=all` para asegurarse de que el broker confirma la recepción, y `flush()` al final para vaciar el buffer.

**Consumidor** (`kafka-consumer-confluent_g2.py`): simplemente se suscribe al topic y va imprimiendo los mensajes que llegan. Lo usamos para comprobar que el productor funciona bien antes de meternos con Spark.

**Spark Streaming** (`struct_kafka_consumer_local_g2.py`): crea una sesión de Spark en local, se conecta al topic de Kafka, guarda el stream en memoria como tabla `raw_data` y ejecuta las dos consultas.

---

## Las dos consultas

### Consulta 1 — Conteo por producto
Agrupa todos los mensajes por producto y cuenta cuántos hay de cada uno. Usa `outputMode("complete")` porque es una agregación: en cada micro-batch Spark tiene que reescribir la tabla entera con los totales actualizados.

Resultado (`salida1.txt`):
```
      value  total
       book     13
  batteries      9
   t-shirts      8
alarm clock      7
  gift card      3
```

### Consulta 2 — Filtrado de productos
Filtra solo los mensajes de `book`, `alarm clock` y `t-shirts`. Usa `outputMode("append")` porque no hay agregación: cada mensaje se evalúa por separado y si pasa el filtro se añade al resultado.

Resultado en `salida2.txt` con las columnas `usuario` y `producto`.

---

## Por qué la primera iteración sale vacía

Es algo que puede sorprender la primera vez: al hacer `SELECT * FROM raw_data` en la iteración 1, la tabla aparece vacía. Esto pasa porque `.start()` lanza la consulta en segundo plano y Spark todavía está negociando la conexión con Kafka y procesando el primer micro-batch. En la iteración 2 ya aparecen los datos.

---

## Dependencias

```
pyspark==4.1.1
confluent-kafka
java-21
```

El conector Spark-Kafka (`spark-sql-kafka-0-10_2.13:4.1.1`) se descarga solo la primera vez que ejecutas el script, no hace falta instalarlo a mano.

La ruta de Java está hardcodeada al principio del script:
```python
os.environ["JAVA_HOME"] = "/usr/lib/jvm/java-21-openjdk-amd64"
```
Si tu instalación está en otro sitio, cámbiala.
