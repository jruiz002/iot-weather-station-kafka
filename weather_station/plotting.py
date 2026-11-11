"""Gráfica en vivo de la telemetría consumida."""

from __future__ import annotations

from datetime import datetime

import matplotlib.dates as mdates
import matplotlib.pyplot as plt

from weather_station.sensors import WIND_DIRECTIONS, WeatherReading


class LivePlot:
    """Mantiene el histórico de lecturas y redibuja la figura con cada dato nuevo."""

    def __init__(self, title: str) -> None:
        self.timestamps: list[datetime] = []
        self.all_temp: list[float] = []
        self.all_hume: list[int] = []
        self.all_wind: list[str] = []

        plt.ion()
        self.fig, (self.ax_temp, self.ax_hum, self.ax_wind) = plt.subplots(3, 1, sharex=True, figsize=(10, 8))
        self.fig.suptitle(title)
        (self._temp_line,) = self.ax_temp.plot([], [], marker="o", color="tab:red")
        (self._hum_line,) = self.ax_hum.plot([], [], marker="o", color="tab:blue")
        (self._wind_line,) = self.ax_wind.plot([], [], marker="s", linestyle="", color="tab:green")

        self.ax_temp.set_ylabel("Temperatura (°C)")
        self.ax_temp.set_ylim(0, 110)
        self.ax_hum.set_ylabel("Humedad (%)")
        self.ax_hum.set_ylim(0, 100)
        self.ax_wind.set_ylabel("Dirección viento")
        self.ax_wind.set_yticks(range(len(WIND_DIRECTIONS)), WIND_DIRECTIONS)
        self.ax_wind.set_ylim(-0.5, len(WIND_DIRECTIONS) - 0.5)
        self.ax_wind.set_xlabel("Hora de recepción")
        self.ax_wind.xaxis.set_major_locator(mdates.AutoDateLocator())
        self.ax_wind.xaxis.set_major_formatter(mdates.DateFormatter("%H:%M:%S"))
        for ax in (self.ax_temp, self.ax_hum, self.ax_wind):
            ax.grid(alpha=0.3)
        self.fig.tight_layout()

    @property
    def is_open(self) -> bool:
        return plt.fignum_exists(self.fig.number)

    def add(self, reading: WeatherReading, timestamp: datetime) -> None:
        self.timestamps.append(timestamp)
        self.all_temp.append(reading.temperatura)
        self.all_hume.append(reading.humedad)
        self.all_wind.append(reading.direccion_viento)
        self._redraw()

    def _redraw(self) -> None:
        self._temp_line.set_data(self.timestamps, self.all_temp)
        self._hum_line.set_data(self.timestamps, self.all_hume)
        self._wind_line.set_data(self.timestamps, [WIND_DIRECTIONS.index(w) for w in self.all_wind])
        for ax in (self.ax_temp, self.ax_hum, self.ax_wind):
            ax.relim()
            ax.autoscale_view(scaley=False)
        self.fig.autofmt_xdate()
        self.fig.canvas.draw_idle()

    def refresh(self, seconds: float = 0.1) -> None:
        """Procesa eventos de la GUI para mantener la ventana responsiva."""
        plt.pause(seconds)
