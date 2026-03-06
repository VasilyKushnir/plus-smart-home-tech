package ru.yandex.practicum.kafka.telemetry.collector.controller;

import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.*;
import ru.yandex.practicum.kafka.telemetry.collector.dto.hub.HubEvent;
import ru.yandex.practicum.kafka.telemetry.collector.dto.sensor.SensorEvent;
import ru.yandex.practicum.kafka.telemetry.collector.service.CollectorService;

@RestController
@RequiredArgsConstructor
@RequestMapping("/events")
public class EventController {
    private final CollectorService collectorService;

    @PostMapping("/sensors")
    @ResponseStatus(HttpStatus.OK)
    public void collectSensorEvent(@RequestBody @Valid SensorEvent sensorEvent) {
        collectorService.collectSensorEvent(sensorEvent);
    }

    @PostMapping("/hubs")
    @ResponseStatus(HttpStatus.OK)
    public void collectHubEvent(@RequestBody @Valid HubEvent hubevent) {
        collectorService.collectHubEvent(hubevent);
    }
}
