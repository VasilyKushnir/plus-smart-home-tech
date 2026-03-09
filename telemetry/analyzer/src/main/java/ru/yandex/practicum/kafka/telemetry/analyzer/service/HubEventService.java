package ru.yandex.practicum.kafka.telemetry.analyzer.service;

import ru.yandex.practicum.kafka.telemetry.event.HubEventAvro;

public interface HubEventService {
    void handleHubEvent(HubEventAvro hubEvent);
}
