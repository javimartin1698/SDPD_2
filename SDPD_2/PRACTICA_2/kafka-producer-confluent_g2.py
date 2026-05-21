#!/usr/bin/env python

# Importamos 'choice' para elegir elementos aleatorios de una lista
# y Producer de confluent_kafka para publicar mensajes en Kafka
from random import choice
from confluent_kafka import Producer

# ─── CONFIGURACIÓN DEL BROKER ───────────────────────────────────────────────

BOOTSTRAP_SERVERS_LOCAL = "localhost:9092"
BOOTSTRAP_SERVERS_LAB   = "localhost:9092"  # Grupo 2

config = {
    # Dirección del broker al que nos conectamos
    "bootstrap.servers": BOOTSTRAP_SERVERS_LAB,

    # "all" el broker líder espera confirmación de TODAS
    # las réplicas antes de dar el mensaje por enviado
    # Otras opciones: "0" (no espera nada) o "1" (solo el líder confirma).
    "acks": "all"
}

# ─── DATOS DE EJEMPLO ────────────────────────────────────────────────────────

TOPIC     = "purchases"  # Topic al que vamos a publicar

# Lista de usuarios (claves de los mensajes)
USER_IDS  = ["eabara-2", "jsmith-2", "sgarcia-2", "jbernard-2", "htanaka-2", "awalther-2"]

# Lista de productos (valores de los mensajes)
PRODUCTS  = ["book", "alarm clock", "t-shirts", "gift card", "batteries"]

# Número de mensajes que enviamos por cada ejecución del script
NUM_MSGS  = 10

# ─── CALLBACK DE CONFIRMACIÓN ────────────────────────────────────────────────

def delivery_callback(err, msg):
    """
    Kafka llama a esta función automáticamente cuando sabe si el mensaje
    llegó bien o falló. Es asíncrono: no bloquea el bucle principal.
    """
    if err:
        # El mensaje NO llegó al broker imprimimos por pantalla ERROR:...
        print(f"ERROR: Message failed delivery: {err}")
    else:
        # El mensaje SÍ llegó entonces mostramos topic, clave y valor
        print(
            f"Produced event to topic {msg.topic()}: "
            f"key = {msg.key().decode('utf-8'):12} "
            f"value = {msg.value().decode('utf-8'):12}"
        )

# ─── MAIN ────────────────────────────────────────────────────────────────────

if __name__ == "__main__":

    # Instanciamos el productor con la configuración del broker
    producer = Producer(config)

    count = 0  # Contador de mensajes encolados

    try:
        for _ in range(NUM_MSGS):
            user_id = choice(USER_IDS)  # Usuario aleatorio (KEY)
            product = choice(PRODUCTS)  # Producto aleatorio (VALUE)

            # produce() encola el mensaje internamente; NO lo envía al instante.
            # Kafka agrupa mensajes en batches para mayor eficiencia.
            # El callback se disparará más tarde cuando haya confirmación.
            producer.produce(TOPIC, product, user_id, callback=delivery_callback)
            count += 1

    except KeyboardInterrupt:
        # Ctrl+C para salir del bucle
        print("\nInterrumpido por el usuario.")

    finally:
        # produce() es asíncrono, los mensajes aún pueden estar
        # en el buffer interno del productor al salir del bucle.

        # poll() procesa los eventos pendientes del broker
        # Le damos 10 segundos de margen para recibir las confirmaciones.
        producer.poll(10000)

        # flush() bloquea el hilo hasta que TODOS los mensajes del buffer
        # han sido enviados o fallan. Sin esto podríamos perder mensajes.
        producer.flush()

        print(f"\nTotal de mensajes enviados: {count}")
