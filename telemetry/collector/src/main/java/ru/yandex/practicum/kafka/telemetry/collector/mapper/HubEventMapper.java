package ru.yandex.practicum.kafka.telemetry.collector.mapper;

import org.apache.avro.specific.SpecificRecordBase;
import ru.yandex.practicum.grpc.telemetry.event.*;
import ru.yandex.practicum.kafka.telemetry.event.*;

import java.time.Instant;
import java.util.List;

public class HubEventMapper {
    public static HubEventAvro toHubEventAvro(HubEventProto hubEvent) {
        HubEventProto.PayloadCase payloadCase = hubEvent.getPayloadCase();
        SpecificRecordBase payload;
        switch (payloadCase) {
            case DEVICE_ADDED -> {
                DeviceAddedEventProto deviceAddedEvent = hubEvent.getDeviceAdded();
                payload = DeviceAddedEventAvro.newBuilder()
                        .setId(deviceAddedEvent.getId())
                        .setType(DeviceTypeAvro.valueOf(deviceAddedEvent.getType().name()))
                        .build();
            }
            case DEVICE_REMOVED -> {
                DeviceRemovedEventProto deviceRemovedEvent = hubEvent.getDeviceRemoved();
                payload = DeviceRemovedEventAvro.newBuilder()
                        .setId(deviceRemovedEvent.getId())
                        .build();
            }
            case SCENARIO_ADDED -> {
                ScenarioAddedEventProto scenarioAddedEvent = hubEvent.getScenarioAdded();
                List<ScenarioConditionAvro> scenarioConditionAvroList = scenarioAddedEvent.getConditionList().stream()
                        .map(c -> {
                                    ScenarioConditionAvro.Builder builder = ScenarioConditionAvro.newBuilder()
                                            .setSensorId(c.getSensorId())
                                            .setType(ConditionTypeAvro.valueOf(c.getType().name()))
                                            .setOperation(ConditionOperationAvro.valueOf(c.getOperation().name()));

                                    switch (c.getValueCase()) {
                                        case ScenarioConditionProto.ValueCase.BOOL_VALUE -> builder.setValue(c.getBoolValue());
                                        case ScenarioConditionProto.ValueCase.INT_VALUE -> builder.setValue(c.getIntValue());
                                    }

                                    return builder.build();
                                }
                        ).toList();

                List<DeviceActionAvro> deviceActionAvroList = scenarioAddedEvent.getActionList().stream()
                        .map(a -> DeviceActionAvro.newBuilder()
                                .setSensorId(a.getSensorId())
                                .setType(ActionTypeAvro.valueOf(a.getType().name()))
                                .setValue(a.getValue())
                                .build()).toList();

                payload = ScenarioAddedEventAvro.newBuilder()
                        .setName(scenarioAddedEvent.getName())
                        .setConditions(scenarioConditionAvroList)
                        .setActions(deviceActionAvroList)
                        .build();
            }
            case SCENARIO_REMOVED -> {
                ScenarioRemovedEventProto scenarioRemovedEvent = hubEvent.getScenarioRemoved();
                payload = ScenarioRemovedEventAvro.newBuilder()
                        .setName(scenarioRemovedEvent.getName())
                        .build();
            }
            default -> {
                throw new IllegalArgumentException("Unsupported hub event type: " + payloadCase);
            }
        }
        return HubEventAvro.newBuilder()
                .setHubId(hubEvent.getHubId())
                .setTimestamp(
                        Instant.ofEpochSecond(
                                hubEvent.getTimestamp().getSeconds(),
                                hubEvent.getTimestamp().getNanos()
                        )
                )
                .setPayload(payload)
                .build();
    }
}
