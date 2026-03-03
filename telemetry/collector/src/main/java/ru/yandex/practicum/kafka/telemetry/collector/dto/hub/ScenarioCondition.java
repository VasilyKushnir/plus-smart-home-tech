package ru.yandex.practicum.kafka.telemetry.collector.dto.hub;

public class ScenarioCondition {
    String sensorId;
    ScenarioConditionType type;
    ScenarioOperationType operation;
    int value;
}
