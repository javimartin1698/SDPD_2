#!/usr/bin/env python

# Importamos el Consumer de la librería confluent_kafka
# Esta librería nos permite comunicarnos con un broker de Apache Kafka
from confluent_kafka import Consumer

# ─── CONFIGURACIÓN DEL BROKER ───────────────────────────────────────────────

# Dirección del broker Kafka. En local y en el lab usamos el mismo puerto 9092.
# En un entorno real estos serían IPs/hosts distintos.
BOOTSTRAP_SERVERS_LOCAL = "localhost:9092"
BOOTSTRAP_SERVERS_LAB   = "localhost:9092"  # Grupo 2

# Diccionario de configuración para el consumidor
config = {
    # Indicamos dónde está el broker (o lista de brokers separados por comas)
    "bootstrap.servers":  BOOTSTRAP_SERVERS_LAB,

    # ID del grupo de consumidores. Kafka usa esto para repartir particiones
    # entre los miembros del mismo grupo y hacer seguimiento del offset.
    "group.id":           "kafka-grupo2-consumer",

    # Desactivamos el commit automático para controlar nosotros cuándo
    # confirmamos que hemos procesado cada mensaje (mayor control y seguridad).
    "enable.auto.commit": "false",

    # Si el grupo no tiene offset guardado (primera vez), empezamos
    # desde el mensaje más antiguo disponible en el topic.
    "auto.offset.reset":  "earliest"
}

# Nombre del topic del que vamos a consumir mensajes
TOPIC = "purchases"

# ─── MAIN ────────────────────────────────────────────────────────────────────

if __name__ == "__main__":

    # Instanciamos el consumidor con la configuración definida arriba
    consumer = Consumer(config)

    # Nos suscribimos al topic. Se puede pasar una lista con varios topics.
    consumer.subscribe([TOPIC])

    print(f"Suscrito al topic '{TOPIC}'. Esperando mensajes... (Ctrl+C para salir)\n")

    try:
        while True:
            # poll() bloquea el hilo hasta 1 segundo esperando un mensaje.
            # Si llega antes, lo devuelve inmediatamente.
            msg = consumer.poll(1.0)

            if msg is None:
                # No llegó ningún mensaje en ese segundo → seguimos esperando
                print("Waiting...")

            elif msg.error():
                # Hubo un error de Kafka (partición no disponible, etc.)
                print(f"ERROR: {msg.error()}")

            else:
                # Mensaje recibido. Lo decodificamos de bytes a string (UTF-8)
                # y mostramos la clave (key) y el contenido (value).
                print(
                    f"Consumed event from topic {msg.topic()}: "
                    f"key = {msg.key().decode('utf-8'):12} "
                    f"value = {msg.value().decode('utf-8'):12}"
                )

    except KeyboardInterrupt:
        # Pulsamos CRT+C y salimos
        print("\nInterrumpido por el usuario. Cerrando consumidor...")
        consumer.unsubscribe()  # Nos desuscribimos del topic

    finally:
        # El bloque finally se ejecuta SIEMPRE, haya error o no.
        # consumer.close() libera el offset en el broker y saca al consumidor
        # del grupo de forma ordenada.
       
        consumer.close()
        print("Consumidor cerrado correctamente.")
