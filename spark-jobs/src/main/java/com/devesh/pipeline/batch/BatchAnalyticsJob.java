package com.devesh.pipeline.batch;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.*;

/**
 * Batch Analytics Spark Job
 * Reads raw data from S3, aggregates, and writes results
 * Author: Devesh Alukuri
 */
public class BatchAnalyticsJob {

    public static void main(String[] args) {
        SparkConf conf = new SparkConf()
                .setAppName("BatchDataAnalytics")
                .setMaster(args.length > 0 ? args[0] : "local[*]");

        SparkSession spark = SparkSession.builder()
                .config(conf)
                .getOrCreate();

        spark.sparkContext().setLogLevel("WARN");

        System.out.println("Starting batch analytics job...");

        // Read raw data from S3
        String inputPath = args.length > 1 ? args[1] : "s3://devesh-pipeline/processed/";
        Dataset<Row> rawData = spark.read().json(inputPath);

        System.out.println("Total records loaded: " + rawData.count());

        // Aggregations
        rawData.groupBy("eventType")
               .count()
               .orderBy(functions.desc("count"))
               .show();

        // Write aggregated results back to S3
        String outputPath = args.length > 2 ? args[2] : "s3://devesh-pipeline/aggregated/";
        rawData.write()
               .mode(SaveMode.Overwrite)
               .partitionBy("date")
               .parquet(outputPath);

        System.out.println("Batch job completed. Results written to: " + outputPath);
        spark.stop();
    }
}
