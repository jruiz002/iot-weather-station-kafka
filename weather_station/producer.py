"""Kafka Producer: simula el dispositivo IoT que envía telemetría al Edge.

Uso:
    python -m weather_station.producer --topic <carne> [--mode json|compact]
"""

from __future__ import annotations

import argparse
import logging
import random
import time

from kafka import KafkaProducer

from weather_station.codec import CODECS, Codec, get_codec
from weather_station.config import DEFAULT_KEY, DEFAULT_TOPIC, KafkaSettings
from weather_station.sensors import WeatherSensor

logger = logging.getLogger("producer")


def build_producer(settings: KafkaSettings) -> KafkaProducer:
    return KafkaProducer(**settings.client_kwargs(), acks="all")


def run(producer: KafkaProducer, sensor: WeatherSensor, codec: Codec, topic: str, key: str,
        min_interval: float, max_interval: float) -> None:
    """Genera y publica lecturas hasta ser interrumpido (Ctrl+C)."""
    while True:
        reading = sensor.read()
        payload = codec.encode(reading)
        metadata = producer.send(topic, key=key.encode(), value=payload).get(timeout=10)
        logger.info("Lectura generada: %s", reading.to_dict())
        logger.info("Enviado [partición=%d, offset=%d] %d bytes: %r",
                    metadata.partition, metadata.offset, len(payload), payload)
        time.sleep(random.uniform(min_interval, max_interval))


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Productor de telemetría meteorológica")
    parser.add_argument("--topic", default=DEFAULT_TOPIC, required=not DEFAULT_TOPIC,
                        help="Topic único de la pareja (número de carné)")
    parser.add_argument("--mode", choices=list(CODECS), default="json", help="Formato del payload")
    parser.add_argument("--key", default=DEFAULT_KEY, help="Key del mensaje")
    parser.add_argument("--min-interval", type=float, default=15.0, help="Segundos mínimos entre envíos")
    parser.add_argument("--max-interval", type=float, default=30.0, help="Segundos máximos entre envíos")
    parser.add_argument("--seed", type=int, default=None, help="Semilla para reproducibilidad")
    return parser.parse_args()


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(name)s] %(message)s")
    logging.getLogger("kafka").setLevel(logging.ERROR)
    args = parse_args()

    producer = build_producer(KafkaSettings())
    logger.info("Publicando en topic '%s' con formato '%s'. Ctrl+C para detener.", args.topic, args.mode)
    try:
        run(producer, WeatherSensor(args.seed), get_codec(args.mode), args.topic, args.key,
            args.min_interval, args.max_interval)
    except KeyboardInterrupt:
        logger.info("Productor detenido por el usuario.")
    finally:
        producer.flush()
        producer.close()


if __name__ == "__main__":
    main()
