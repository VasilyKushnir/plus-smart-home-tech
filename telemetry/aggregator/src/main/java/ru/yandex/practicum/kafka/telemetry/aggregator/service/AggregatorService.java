package ru.yandex.practicum.kafka.telemetry.aggregator.service;

import ru.yandex.practicum.kafka.telemetry.event.SensorEventAvro;

public interface AggregatorService {
    void handleEvent(SensorEventAvro sensorEvent);
}
