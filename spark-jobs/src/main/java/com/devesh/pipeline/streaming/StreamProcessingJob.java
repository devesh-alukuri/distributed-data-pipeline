package com.devesh.pipeline.streaming;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.*;
import org.apache.spark.streaming.kafka010.*;

import java.util.*;

/**
 * Real-time Spark Streaming Job
 * Reads from Kafka, processes events, writes to S3
 * Author: Devesh Alukuri
 */
public class StreamProcessingJob {

    public static void main(String[] args) throws Exception {
        SparkConf conf = new SparkConf()
                .setAppName("RealTimeDataProcessor")
                .setMaster(args.length > 0 ? args[0] : "local[*]");

        JavaStreamingContext ssc = new JavaStreamingContext(conf, Durations.seconds(5));

        // Enable checkpointing for fault tolerance
        ssc.checkpoint("s3://devesh-pipeline/checkpoints/stream/");

        // Kafka parameters
        Map<String, Object> kafkaParams = new HashMap<>();
        kafkaParams.put("bootstrap.servers", "localhost:9092");
        kafkaParams.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        kafkaParams.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        kafkaParams.put("group.id", "spark-streaming-group");
        kafkaParams.put("auto.offset.reset", "latest");
        kafkaParams.put("enable.auto.commit", false);

        Collection<String> topics = Arrays.asList("raw-events", "iot-events");

        // Create Kafka direct stream
        JavaInputDStream<ConsumerRecord<String, String>> stream =
                KafkaUtils.createDirectStream(
                        ssc,
                        LocationStrategies.PreferConsistent(),
                        ConsumerStrategies.Subscribe(topics, kafkaParams)
                );

        // Process each RDD
        stream
            .map(record -> record.value())
            .filter(msg -> msg != null && !msg.isEmpty())
            .map(StreamProcessingJob::enrichEvent)
            .foreachRDD(rdd -> {
                if (!rdd.isEmpty()) {
                    long count = rdd.count();
                    System.out.println("Processing batch of " + count + " events");

                    // Write to S3
                    String outputPath = "s3://devesh-pipeline/processed/" +
                            java.time.LocalDate.now() + "/";
                    rdd.saveAsTextFile(outputPath);

                    // Commit Kafka offsets after successful write
                    OffsetRange[] offsets = ((HasOffsetRanges) rdd.rdd()).offsetRanges();
                    ((CanCommitOffsets) stream.inputDStream()).commitAsync(offsets);
                }
            });

        ssc.start();
        ssc.awaitTermination();
    }

    private static String enrichEvent(String rawEvent) {
        // Add processing metadata to event
        return rawEvent.replace("}", ",\"processedBy\":\"spark-stream\","
                + "\"processedAt\":\"" + java.time.LocalDateTime.now() + "\"}");
    }
}
