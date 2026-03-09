package ru.yandex.practicum.kafka.telemetry.analyzer.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import ru.yandex.practicum.kafka.telemetry.analyzer.model.Action;
import ru.yandex.practicum.kafka.telemetry.analyzer.model.Condition;
import ru.yandex.practicum.kafka.telemetry.analyzer.model.Scenario;
import ru.yandex.practicum.kafka.telemetry.analyzer.model.Sensor;
import ru.yandex.practicum.kafka.telemetry.analyzer.repository.ScenarioRepository;
import ru.yandex.practicum.kafka.telemetry.analyzer.repository.SensorRepository;
import ru.yandex.practicum.kafka.telemetry.event.*;

import java.util.stream.Collectors;

@Slf4j
@Service
@RequiredArgsConstructor
public class HubEventServiceImpl implements HubEventService {

    private final SensorRepository sensorRepository;
    private final ScenarioRepository scenarioRepository;

    @Override
    public void handleHubEvent(HubEventAvro hubEvent) {
        Object payload = hubEvent.getPayload();
        switch (payload) {
            case DeviceAddedEventAvro event -> {
                if (!sensorRepository.existsByIdAndHubId(event.getId(), hubEvent.getHubId())) {
                    log.info("Add sensor: {}", hubEvent);
                    sensorRepository.save(Sensor.builder()
                            .id(event.getId())
                            .hubId(hubEvent.getHubId())
                            .build());
                }
            }
            case DeviceRemovedEventAvro event -> {
                log.info("Remove sensor: {}", hubEvent);
                sensorRepository.deleteById(event.getId());
            }
            case ScenarioAddedEventAvro event -> {
                log.info("Add/update scenario: {}", hubEvent);
                Scenario scenario = scenarioRepository
                        .findByHubIdAndName(hubEvent.getHubId(), event.getName())
                        .orElse(Scenario.builder()
                                .hubId(hubEvent.getHubId())
                                .name(event.getName()).build());

                scenario.setActions(event.getActions()
                        .stream()
                        .collect(Collectors.toMap(DeviceActionAvro::getSensorId, a -> Action.builder()
                                .type(a.getType())
                                .value(a.getValue())
                                .build())));

                scenario.setConditions(event.getConditions()
                        .stream()
                        .collect(Collectors.toMap(ScenarioConditionAvro::getSensorId, c -> Condition.builder()
                                .type(c.getType())
                                .operation(c.getOperation())
                                .value(convertToInteger(c.getValue()))
                                .build())));

                scenarioRepository.save(scenario);
            }
            case ScenarioRemovedEventAvro event -> {
                log.info("Remove scenario: {}", hubEvent);
                scenarioRepository.deleteByName(event.getName());
            }
            default -> throw new IllegalArgumentException("Unknown payload type: " + payload);
        }
    }

    private Integer convertToInteger(Object value) {
        return switch (value) {
            case null -> null;
            case Integer i -> i;
            case Boolean b -> b ? 1 : 0;
            default -> throw new RuntimeException("Unknown value type: " + value);
        };
    }
}
