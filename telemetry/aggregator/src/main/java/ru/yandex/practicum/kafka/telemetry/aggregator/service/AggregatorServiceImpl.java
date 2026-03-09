package ru.yandex.practicum.kafka.telemetry.aggregator.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import ru.yandex.practicum.kafka.telemetry.aggregator.kafka.KafkaClient;
import ru.yandex.practicum.kafka.telemetry.event.SensorEventAvro;
import ru.yandex.practicum.kafka.telemetry.event.SensorStateAvro;
import ru.yandex.practicum.kafka.telemetry.event.SensorsSnapshotAvro;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

@Slf4j
@RequiredArgsConstructor
@Service
public class AggregatorServiceImpl implements AggregatorService {
    private final KafkaClient kafkaClient;
    private final Map<String, SensorsSnapshotAvro> snapshots = new HashMap<>();

    @Value("${telemetry.kafka.aggregator.topic.snapshots}")
    private String topic;

    @Override
    public void handleEvent(SensorEventAvro sensorEvent) {
        log.info("Received event {}", sensorEvent);

        Optional<SensorsSnapshotAvro> sensorsSnapshotAvroOptional = updateState(sensorEvent);
        if (sensorsSnapshotAvroOptional.isPresent()) {
            SensorsSnapshotAvro sensorSnapshotAvro = sensorsSnapshotAvroOptional.get();

            log.info("Saving sensor state {}", sensorSnapshotAvro);

            kafkaClient.send(topic, sensorSnapshotAvro, sensorSnapshotAvro.getTimestamp(),
                    sensorSnapshotAvro.getHubId());
        }
    }

    private Optional<SensorsSnapshotAvro> updateState(SensorEventAvro sensorEvent) {
        if (!snapshots.containsKey(sensorEvent.getHubId())) {
            SensorsSnapshotAvro sensorsSnapshotAvro = SensorsSnapshotAvro.newBuilder()
                    .setHubId(sensorEvent.getHubId())
                    .setSensorsState(new HashMap<>())
                    .setTimestamp(sensorEvent.getTimestamp())
                    .build();
            snapshots.put(sensorEvent.getHubId(), sensorsSnapshotAvro);
        }

        SensorsSnapshotAvro sensorSnapshotAvro = snapshots.get(sensorEvent.getHubId());

        if (sensorSnapshotAvro.getSensorsState().containsKey(sensorEvent.getId())) {
            SensorStateAvro oldState = sensorSnapshotAvro.getSensorsState().get(sensorEvent.getId());
            if (oldState.getTimestamp().isAfter(sensorEvent.getTimestamp()) ||
                    oldState.getData().equals(sensorEvent.getPayload())) {
                return Optional.empty();
            }
        }

        SensorStateAvro newState = SensorStateAvro.newBuilder()
                .setTimestamp(sensorEvent.getTimestamp())
                .setData(sensorEvent.getPayload())
                .build();

        sensorSnapshotAvro.getSensorsState().put(sensorEvent.getId(), newState);
        sensorSnapshotAvro.setTimestamp(sensorEvent.getTimestamp());

        return Optional.of(sensorSnapshotAvro);
    }
}
