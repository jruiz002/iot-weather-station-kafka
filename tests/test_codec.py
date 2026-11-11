import pytest

from weather_station.codec import CompactCodec, JsonCodec
from weather_station.sensors import WIND_DIRECTIONS, WeatherReading, WeatherSensor


@pytest.mark.parametrize("codec", [JsonCodec(), CompactCodec()])
def test_round_trip_random_readings(codec):
    sensor = WeatherSensor(seed=42)
    for _ in range(1000):
        reading = sensor.read()
        assert codec.decode(codec.encode(reading)) == reading


@pytest.mark.parametrize("direction", WIND_DIRECTIONS)
def test_compact_extremes(direction):
    codec = CompactCodec()
    for reading in (WeatherReading(0.0, 0, direction), WeatherReading(110.0, 100, direction)):
        payload = codec.encode(reading)
        assert len(payload) == 3
        assert codec.decode(payload) == reading


def test_compact_rejects_out_of_range():
    codec = CompactCodec()
    with pytest.raises(ValueError):
        codec.encode(WeatherReading(110.01, 50, "N"))
    with pytest.raises(ValueError):
        codec.encode(WeatherReading(50.0, 101, "N"))
    with pytest.raises(ValueError):
        codec.encode(WeatherReading(50.0, 50, "X"))


def test_compact_rejects_wrong_size():
    with pytest.raises(ValueError):
        CompactCodec().decode(b'{"temperatura": 1}')
