"""Simulación de los sensores de la estación meteorológica."""

from __future__ import annotations

import random
from dataclasses import asdict, dataclass

WIND_DIRECTIONS: tuple[str, ...] = ("N", "NO", "O", "SO", "S", "SE", "E", "NE")

TEMPERATURE_RANGE = (0.0, 110.0)
HUMIDITY_RANGE = (0, 100)

# Distribución normal centrada en 55 (punto medio de [0, 110]).
MEAN = 55.0
STD_DEV = 15.0


@dataclass(frozen=True)
class WeatherReading:
    """Una medición de la estación."""

    temperatura: float
    humedad: int
    direccion_viento: str

    def to_dict(self) -> dict[str, float | int | str]:
        return asdict(self)

    @classmethod
    def from_dict(cls, data: dict) -> "WeatherReading":
        return cls(
            temperatura=float(data["temperatura"]),
            humedad=int(data["humedad"]),
            direccion_viento=str(data["direccion_viento"]),
        )


def _clamp(value: float, low: float, high: float) -> float:
    return max(low, min(high, value))


class WeatherSensor:
    """Genera lecturas pseudoaleatorias para los tres sensores."""

    def __init__(self, seed: int | None = None) -> None:
        self._rng = random.Random(seed)

    def read_temperature(self) -> float:
        value = self._rng.gauss(MEAN, STD_DEV)
        return round(_clamp(value, *TEMPERATURE_RANGE), 2)

    def read_humidity(self) -> int:
        value = round(self._rng.gauss(MEAN, STD_DEV))
        return int(_clamp(value, *HUMIDITY_RANGE))

    def read_wind_direction(self) -> str:
        return self._rng.choice(WIND_DIRECTIONS)

    def read(self) -> WeatherReading:
        return WeatherReading(
            temperatura=self.read_temperature(),
            humedad=self.read_humidity(),
            direccion_viento=self.read_wind_direction(),
        )
