package ru.yandex.practicum.kafka.telemetry.collector.service;

import ru.yandex.practicum.grpc.telemetry.event.HubEventProto;
import ru.yandex.practicum.grpc.telemetry.event.SensorEventProto;

public interface CollectorService {
    void collectSensorEvent(SensorEventProto sensorEvent);

    void collectHubEvent(HubEventProto hubEvent);
}
