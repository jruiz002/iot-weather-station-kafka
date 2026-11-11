"""Kafka Consumer: recibe la telemetría y la grafica en tiempo real.

Uso:
    python -m weather_station.consumer --topic <carne> [--mode json|compact]
"""

from __future__ import annotations

import argparse
import logging
from datetime import datetime

from kafka import KafkaConsumer

from weather_station.codec import CODECS, Codec, get_codec
from weather_station.config import DEFAULT_TOPIC, KafkaSettings
from weather_station.plotting import LivePlot

logger = logging.getLogger("consumer")


def build_consumer(settings: KafkaSettings, topic: str, group_id: str, offset_reset: str) -> KafkaConsumer:
    return KafkaConsumer(
        topic,
        **settings.client_kwargs(),
        group_id=group_id,
        auto_offset_reset=offset_reset,
        # Sin commit de offsets: en cada ejecución se vuelve a graficar el
        # histórico según ``auto_offset_reset`` además de los datos entrantes.
        enable_auto_commit=False,
    )


def run(consumer: KafkaConsumer, codec: Codec, plot: LivePlot) -> None:
    """Consume mensajes y actualiza la gráfica hasta que se cierre la ventana."""
    while plot.is_open:
        batches = consumer.poll(timeout_ms=500)
        for records in batches.values():
            for message in records:
                try:
                    reading = codec.decode(message.value)
                except (ValueError, KeyError, UnicodeDecodeError) as exc:
                    logger.warning("Mensaje ignorado (offset %d): %s", message.offset, exc)
                    continue
                logger.info("Recibido offset=%d (%d bytes) -> %s",
                            message.offset, len(message.value), reading.to_dict())
                plot.add(reading, datetime.fromtimestamp(message.timestamp / 1000))
        plot.refresh()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Consumidor y graficador de telemetría meteorológica")
    parser.add_argument("--topic", default=DEFAULT_TOPIC, required=not DEFAULT_TOPIC,
                        help="Topic único de la pareja (número de carné)")
    parser.add_argument("--mode", choices=list(CODECS), default="json", help="Formato del payload")
    parser.add_argument("--group-id", default=None, help="Consumer group (por defecto: <topic>-<mode>)")
    parser.add_argument("--offset-reset", choices=["earliest", "latest"], default="earliest",
                        help="Desde dónde leer si el grupo no tiene offsets guardados")
    return parser.parse_args()


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(name)s] %(message)s")
    logging.getLogger("kafka").setLevel(logging.ERROR)
    args = parse_args()
    group_id = args.group_id or f"{args.topic}-{args.mode}"

    consumer = build_consumer(KafkaSettings(), args.topic, group_id, args.offset_reset)
    plot = LivePlot(f"Estación meteorológica — topic {args.topic} ({args.mode})")
    logger.info("Escuchando topic '%s' (grupo '%s'). Cierre la ventana o Ctrl+C para salir.",
                args.topic, group_id)
    try:
        run(consumer, get_codec(args.mode), plot)
    except KeyboardInterrupt:
        logger.info("Consumidor detenido por el usuario.")
    finally:
        consumer.close()


if __name__ == "__main__":
    main()
