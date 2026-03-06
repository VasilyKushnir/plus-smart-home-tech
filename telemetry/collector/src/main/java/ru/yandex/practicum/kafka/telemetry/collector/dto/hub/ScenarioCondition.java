package ru.yandex.practicum.kafka.telemetry.collector.dto.hub;

import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

@Getter
@Setter
@ToString
public class ScenarioCondition {
    private String sensorId;
    private ScenarioConditionType type;
    private ScenarioOperationType operation;
    private Integer value;
}
