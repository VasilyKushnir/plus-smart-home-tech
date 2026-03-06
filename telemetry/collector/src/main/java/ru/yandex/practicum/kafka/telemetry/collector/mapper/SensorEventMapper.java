package ru.yandex.practicum.kafka.telemetry.collector.mapper;

import org.apache.avro.specific.SpecificRecordBase;
import ru.yandex.practicum.kafka.telemetry.collector.dto.sensor.*;
import ru.yandex.practicum.kafka.telemetry.event.*;

public class SensorEventMapper {
    public static SensorEventAvro toSensorEventAvro(SensorEvent sensorEvent) {
        SpecificRecordBase payload;
        switch (sensorEvent.getType()) {
            case CLIMATE_SENSOR_EVENT -> {
                ClimateSensorEvent climateSensorEvent = (ClimateSensorEvent) sensorEvent;
                payload = ClimateSensorAvro.newBuilder()
                        .setTemperatureC(climateSensorEvent.getTemperatureC())
                        .setHumidity(climateSensorEvent.getHumidity())
                        .setCo2Level(climateSensorEvent.getCo2Level())
                        .build();
            }
            case LIGHT_SENSOR_EVENT -> {
                LightSensorEvent lightSensorEvent = (LightSensorEvent) sensorEvent;
                payload = LightSensorAvro.newBuilder()
                        .setLinkQuality(lightSensorEvent.getLinkQuality())
                        .setLuminosity(lightSensorEvent.getLuminosity())
                        .build();
            }
            case MOTION_SENSOR_EVENT -> {
                MotionSensorEvent motionSensorEvent = (MotionSensorEvent) sensorEvent;
                payload = MotionSensorAvro.newBuilder()
                        .setLinkQuality(motionSensorEvent.getLinkQuality())
                        .setMotion(motionSensorEvent.isMotion())
                        .setVoltage(motionSensorEvent.getVoltage())
                        .build();
            }
            case SWITCH_SENSOR_EVENT -> {
                SwitchSensorEvent switchSensorEvent = (SwitchSensorEvent) sensorEvent;
                payload = SwitchSensorAvro.newBuilder()
                        .setState(switchSensorEvent.isState())
                        .build();
            }
            case TEMPERATURE_SENSOR_EVENT -> {
                TemperatureSensorEvent temperatureSensorEvent = (TemperatureSensorEvent) sensorEvent;
                payload = TemperatureSensorAvro.newBuilder()
                        .setId(temperatureSensorEvent.getId())
                        .setHubId(temperatureSensorEvent.getHubId())
                        .setTimestamp(temperatureSensorEvent.getTimestamp())
                        .setTemperatureC(temperatureSensorEvent.getTemperatureC())
                        .setTemperatureF(temperatureSensorEvent.getTemperatureF())
                        .build();
            }
            default -> {
                throw new IllegalArgumentException("Unsupported sensor event type: " + sensorEvent.getType());
            }
        }
        return SensorEventAvro.newBuilder()
                .setId(sensorEvent.getId())
                .setHubId(sensorEvent.getHubId())
                .setTimestamp(sensorEvent.getTimestamp())
                .setPayload(payload)
                .build();
    }
}
