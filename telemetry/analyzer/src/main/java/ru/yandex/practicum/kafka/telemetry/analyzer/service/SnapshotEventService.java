package ru.yandex.practicum.kafka.telemetry.analyzer.service;

import ru.yandex.practicum.kafka.telemetry.event.SensorsSnapshotAvro;

public interface SnapshotEventService {
    void handleSnapshot(SensorsSnapshotAvro snapshot);
}
