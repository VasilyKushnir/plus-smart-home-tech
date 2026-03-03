package ru.yandex.practicum.kafka.telemetry.collector.service;

import ru.yandex.practicum.kafka.telemetry.collector.dto.hub.HubEvent;
import ru.yandex.practicum.kafka.telemetry.collector.dto.sensor.SensorEvent;

public interface CollectorService {
    void collectSensorEvent(SensorEvent sensorEvent);

    void collectHubEvent(HubEvent hubEvent);
}
