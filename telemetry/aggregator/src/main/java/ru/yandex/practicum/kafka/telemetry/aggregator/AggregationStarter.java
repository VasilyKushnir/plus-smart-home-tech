package ru.yandex.practicum.kafka.telemetry.aggregator;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.stereotype.Component;
import ru.yandex.practicum.kafka.telemetry.event.SensorEventAvro;
import ru.yandex.practicum.kafka.telemetry.aggregator.service.AggregatorServiceImpl;
import ru.yandex.practicum.telemetry.deserializer.SensorEventDeserializer;

import java.time.Duration;
import java.util.List;
import java.util.Properties;

/**
 * Класс AggregationStarter, ответственный за запуск агрегации данных.
 */

@Slf4j
@Component
@RequiredArgsConstructor
public class AggregationStarter {

    private final AggregatorServiceImpl aggregatorService;

    // ... объявление полей и конструктора ...

    /**
     * Метод для начала процесса агрегации данных.
     * Подписывается на топики для получения событий от датчиков,
     * формирует снимок их состояния и записывает в кафку.
     */

    public void start() {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.CLIENT_ID_CONFIG, "SomeConsumer");
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "some.group.id");
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getCanonicalName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, SensorEventDeserializer.class
                .getCanonicalName());

        KafkaConsumer<String, SensorEventAvro> consumer = new KafkaConsumer<>(properties);

        try {

            // ... подготовка к обработке данных ...
            // ... например, подписка на топик ...

            consumer.subscribe(List.of("telemetry.sensors.v1"));

            // Цикл обработки событий
            while (true) {

                ConsumerRecords<String, SensorEventAvro> records = consumer.poll(Duration.ofMillis(500));
                for (ConsumerRecord<String, SensorEventAvro> record : records) {
                    aggregatorService.handleEvent(record.value());
                }
                // ... реализация цикла опроса ...
                // ... и обработка полученных данных ...
            }

        } catch (WakeupException ignored) {
            // игнорируем - закрываем консьюмер и продюсер в блоке finally
        } catch (Exception e) {
            log.error("Ошибка во время обработки событий от датчиков", e);
        } finally {

            try {
                // Перед тем, как закрыть продюсер и консьюмер, нужно убедиться,
                // что все сообщения, лежащие в буффере, отправлены и
                // все оффсеты обработанных сообщений зафиксированы

                // здесь нужно вызвать метод продюсера для сброса данных в буффере
                consumer.commitSync();
                // здесь нужно вызвать метод консьюмера для фиксации смещений

            } finally {
                log.info("Закрываем консьюмер");
                consumer.close();
//                log.info("Закрываем продюсер");
//                producer.close();
            }
        }
    }
}