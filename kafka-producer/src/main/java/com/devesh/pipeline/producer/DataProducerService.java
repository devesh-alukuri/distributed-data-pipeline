package com.devesh.pipeline.producer;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;
import org.springframework.web.bind.annotation.*;

import java.time.LocalDateTime;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

@Service
@RequiredArgsConstructor
@Slf4j
public class DataProducerService {

    private final KafkaTemplate<String, String> kafkaTemplate;
    private final ObjectMapper objectMapper;

    public void publishEvent(String topic, Map<String, Object> eventData) {
        try {
            eventData.put("eventId", UUID.randomUUID().toString());
            eventData.put("publishedAt", LocalDateTime.now().toString());

            String message = objectMapper.writeValueAsString(eventData);

            CompletableFuture<SendResult<String, String>> future =
                    kafkaTemplate.send(topic, eventData.get("eventId").toString(), message);

            future.whenComplete((result, ex) -> {
                if (ex == null) {
                    log.info("Published to topic [{}] partition [{}] offset [{}]",
                            topic,
                            result.getRecordMetadata().partition(),
                            result.getRecordMetadata().offset());
                } else {
                    log.error("Failed to publish to topic [{}]: {}", topic, ex.getMessage());
                }
            });
        } catch (Exception e) {
            log.error("Error serializing event: {}", e.getMessage());
            throw new RuntimeException("Failed to publish event", e);
        }
    }
}

@RestController
@RequestMapping("/api/produce")
@RequiredArgsConstructor
class DataProducerController {

    private final DataProducerService producerService;

    @PostMapping("/{topic}")
    public String publishToTopic(@PathVariable String topic,
                                  @RequestBody Map<String, Object> payload) {
        producerService.publishEvent(topic, payload);
        return "Event published to topic: " + topic;
    }
}
