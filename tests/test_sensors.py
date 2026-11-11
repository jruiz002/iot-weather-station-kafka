from statistics import mean

from weather_station.sensors import WIND_DIRECTIONS, WeatherSensor


def test_readings_within_ranges():
    sensor = WeatherSensor(seed=1)
    for _ in range(5000):
        r = sensor.read()
        assert 0.0 <= r.temperatura <= 110.0
        assert round(r.temperatura, 2) == r.temperatura
        assert 0 <= r.humedad <= 100
        assert isinstance(r.humedad, int)
        assert r.direccion_viento in WIND_DIRECTIONS


def test_distribution_centered_at_55():
    sensor = WeatherSensor(seed=7)
    readings = [sensor.read() for _ in range(10000)]
    assert abs(mean(r.temperatura for r in readings) - 55) < 1
    assert abs(mean(r.humedad for r in readings) - 55) < 1
