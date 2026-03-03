package ru.yandex.practicum.kafka.telemetry.collector.service;

import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import ru.yandex.practicum.kafka.telemetry.collector.dto.hub.HubEvent;
import ru.yandex.practicum.kafka.telemetry.collector.dto.sensor.SensorEvent;

@Service
@RequiredArgsConstructor
public class CollectorServiceImpl implements CollectorService {
    // TODO: method implementation
    @Override
    public void collectSensorEvent(SensorEvent sensorEvent) {
        return;
    }

    // TODO: method implementation
    @Override
    public void collectHubEvent(HubEvent hubEvent) {
        return;
    }
}
