"""Codificación/decodificación de las lecturas para su envío por Kafka.

Se ofrecen dos formatos:

* ``JsonCodec``: representación JSON legible (secciones 3.1 a 3.3).
* ``CompactCodec``: payload restringido a 3 bytes / 24 bits (sección 3.4).

Distribución de bits del formato compacto (big-endian)::

    | temperatura (14 bits) | humedad (7 bits) | dirección viento (3 bits) |
     23                  10   9              3   2                       0

La temperatura se transmite como entero en centésimas de grado
(``round(t * 100)``): el máximo es 110.00 -> 11000 < 2^14 = 16384.
"""

from __future__ import annotations

import json
from typing import Protocol

from weather_station.sensors import HUMIDITY_RANGE, TEMPERATURE_RANGE, WIND_DIRECTIONS, WeatherReading


class Codec(Protocol):
    """Contrato común para todos los formatos de serialización."""

    def encode(self, reading: WeatherReading) -> bytes: ...

    def decode(self, payload: bytes) -> WeatherReading: ...


class JsonCodec:
    """Serializa la lectura como JSON en UTF-8."""

    def encode(self, reading: WeatherReading) -> bytes:
        return json.dumps(reading.to_dict()).encode("utf-8")

    def decode(self, payload: bytes) -> WeatherReading:
        return WeatherReading.from_dict(json.loads(payload.decode("utf-8")))


class CompactCodec:
    """Empaqueta la lectura en exactamente 3 bytes."""

    PAYLOAD_SIZE = 3
    TEMP_BITS, HUM_BITS, WIND_BITS = 14, 7, 3
    TEMP_SCALE = 100  # dos decimales -> centésimas

    _HUM_SHIFT = WIND_BITS
    _TEMP_SHIFT = WIND_BITS + HUM_BITS
    _MAX_TEMP_RAW = int(TEMPERATURE_RANGE[1] * TEMP_SCALE)

    def encode(self, reading: WeatherReading) -> bytes:
        temp_raw = round(reading.temperatura * self.TEMP_SCALE)
        if not 0 <= temp_raw <= self._MAX_TEMP_RAW:
            raise ValueError(f"Temperatura fuera de rango: {reading.temperatura}")
        if not HUMIDITY_RANGE[0] <= reading.humedad <= HUMIDITY_RANGE[1]:
            raise ValueError(f"Humedad fuera de rango: {reading.humedad}")
        try:
            wind_raw = WIND_DIRECTIONS.index(reading.direccion_viento)
        except ValueError as exc:
            raise ValueError(f"Dirección de viento inválida: {reading.direccion_viento}") from exc

        packed = (temp_raw << self._TEMP_SHIFT) | (reading.humedad << self._HUM_SHIFT) | wind_raw
        return packed.to_bytes(self.PAYLOAD_SIZE, byteorder="big")

    def decode(self, payload: bytes) -> WeatherReading:
        if len(payload) != self.PAYLOAD_SIZE:
            raise ValueError(f"Se esperaban {self.PAYLOAD_SIZE} bytes, se recibieron {len(payload)}")

        packed = int.from_bytes(payload, byteorder="big")
        wind_raw = packed & ((1 << self.WIND_BITS) - 1)
        humidity = (packed >> self._HUM_SHIFT) & ((1 << self.HUM_BITS) - 1)
        temp_raw = (packed >> self._TEMP_SHIFT) & ((1 << self.TEMP_BITS) - 1)

        if temp_raw > self._MAX_TEMP_RAW or humidity > HUMIDITY_RANGE[1]:
            raise ValueError(f"Payload inválido: {payload.hex()}")

        return WeatherReading(
            temperatura=temp_raw / self.TEMP_SCALE,
            humedad=humidity,
            direccion_viento=WIND_DIRECTIONS[wind_raw],
        )


CODECS: dict[str, type[Codec]] = {"json": JsonCodec, "compact": CompactCodec}


def get_codec(name: str) -> Codec:
    """Devuelve una instancia del codec solicitado (``json`` o ``compact``)."""
    try:
        return CODECS[name]()
    except KeyError as exc:
        raise ValueError(f"Codec desconocido: {name}. Opciones: {list(CODECS)}") from exc
