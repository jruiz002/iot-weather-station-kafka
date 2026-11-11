"""Configuración de conexión al Kafka Broker (Edge Server).

Las credenciales se leen de variables de entorno o de un archivo ``.env``
local (ver ``.env.example``), de modo que no queden en el código fuente.
"""

from __future__ import annotations

import os
from dataclasses import dataclass

from dotenv import load_dotenv

load_dotenv()


@dataclass(frozen=True)
class KafkaSettings:
    """Parámetros de conexión y autenticación SASL/PLAIN."""

    bootstrap_servers: str = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "161.35.11.141:9092")
    security_protocol: str = os.getenv("KAFKA_SECURITY_PROTOCOL", "SASL_PLAINTEXT")
    sasl_mechanism: str = os.getenv("KAFKA_SASL_MECHANISM", "PLAIN")
    username: str = os.getenv("KAFKA_USERNAME", "")
    password: str = os.getenv("KAFKA_PASSWORD", "")

    def __post_init__(self) -> None:
        if not self.username or not self.password:
            raise RuntimeError(
                "Faltan KAFKA_USERNAME/KAFKA_PASSWORD. Copie .env.example a .env y complete las credenciales."
            )

    def client_kwargs(self) -> dict[str, str]:
        """Argumentos comunes para KafkaProducer y KafkaConsumer."""
        return {
            "bootstrap_servers": self.bootstrap_servers,
            "security_protocol": self.security_protocol,
            "sasl_mechanism": self.sasl_mechanism,
            "sasl_plain_username": self.username,
            "sasl_plain_password": self.password,
        }


DEFAULT_TOPIC = os.getenv("KAFKA_TOPIC", "23719")
DEFAULT_KEY = "sensor1"
