package com.rmc.medals.service;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;

@Service
public class SparkService {

    private SparkSession sparkSession;

    private Dataset<Row> dataFrame;

    @PostConstruct
    public void init() {
        this.sparkSession = SparkSession.builder()
                .config(getSparkConfig())
                .getOrCreate();

        dataFrame = this.sparkSession.read()
                .option("header", "true")
                .option("inferSchema", "true")
                .csv("src/main/resources/data/result.csv");

        dataFrame.createOrReplaceTempView("medals");
    }

    public Dataset<Row> execute(String query) {
        return this.sparkSession.sql(query);
    }

    private SparkConf getSparkConfig() {
        return new SparkConf()
                .setAppName("OlympicMedals")
                .setMaster("local")
                .set("spark.sql.shuffle.partitions", "1");
    }
}