package com.devesh.pipeline.consumer;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Service;

import java.util.Map;

@Service
@RequiredArgsConstructor
@Slf4j
public class DataConsumerService {

    private final ObjectMapper objectMapper;

    @KafkaListener(topics = "raw-events", groupId = "pipeline-group",
                   concurrency = "3", containerFactory = "kafkaListenerContainerFactory")
    public void consumeRawEvents(ConsumerRecord<String, String> record, Acknowledgment ack) {
        try {
            log.info("Received event from topic [{}] partition [{}] offset [{}]",
                    record.topic(), record.partition(), record.offset());

            Map<String, Object> event = objectMapper.readValue(record.value(), Map.class);
            processEvent(event);
            ack.acknowledge(); // manual ack after successful processing

        } catch (Exception e) {
            log.error("Failed to process event at offset [{}]: {}", record.offset(), e.getMessage());
            // In production: route to Dead Letter Topic
        }
    }

    @KafkaListener(topics = "iot-events", groupId = "iot-group")
    public void consumeIotEvents(String message) {
        log.info("IoT event received: {}", message.substring(0, Math.min(100, message.length())));
        // Process IoT telemetry
    }

    private void processEvent(Map<String, Object> event) {
        String eventId = (String) event.getOrDefault("eventId", "unknown");
        log.info("Processing event ID: {}", eventId);

        // Validate event structure
        if (!event.containsKey("publishedAt")) {
            log.warn("Event [{}] missing timestamp, skipping enrichment", eventId);
        }

        // Transform and enrich event
        event.put("processedAt", java.time.LocalDateTime.now().toString());
        event.put("processingNode", "consumer-" + Thread.currentThread().getName());

        log.info("Event [{}] processed successfully", eventId);
        // In production: write to S3 / database / Spark stream
    }
}
