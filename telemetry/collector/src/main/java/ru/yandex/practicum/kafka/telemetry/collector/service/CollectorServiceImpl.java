package ru.yandex.practicum.kafka.telemetry.collector.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import ru.yandex.practicum.grpc.telemetry.event.HubEventProto;
import ru.yandex.practicum.grpc.telemetry.event.SensorEventProto;
import ru.yandex.practicum.kafka.telemetry.collector.kafka.KafkaClient;
import ru.yandex.practicum.kafka.telemetry.collector.mapper.HubEventMapper;
import ru.yandex.practicum.kafka.telemetry.collector.mapper.SensorEventMapper;
import ru.yandex.practicum.kafka.telemetry.event.HubEventAvro;
import ru.yandex.practicum.kafka.telemetry.event.SensorEventAvro;

@Service
@RequiredArgsConstructor
@Slf4j
public class CollectorServiceImpl implements CollectorService {
    private final KafkaClient kafkaClient;

    @Override
    public void collectSensorEvent(SensorEventProto sensorEvent) {
        SensorEventAvro sensorEventAvro = SensorEventMapper.toSensorEventAvro(sensorEvent);
        log.info("Sensor Event {}", sensorEvent);
        kafkaClient.send(
                "telemetry.sensors.v1",
                sensorEventAvro,
                sensorEventAvro.getTimestamp(),
                sensorEventAvro.getHubId());
    }

    @Override
    public void collectHubEvent(HubEventProto hubEvent) {
        HubEventAvro hubEventAvro = HubEventMapper.toHubEventAvro(hubEvent);
        log.info("Hub Event {}", hubEvent);
        kafkaClient.send("telemetry.hubs.v1", hubEventAvro, hubEventAvro.getTimestamp(), hubEventAvro.getHubId());
    }
}
