package com.rmc.medals;

import com.rmc.medals.service.SparkService;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.sum;

import static java.util.Arrays.asList;

@RunWith(SpringRunner.class)
@SpringBootTest
public class OlympicMedalsApplicationTests {

	@Test
	public void contextLoads() {
        SparkConf conf = new SparkConf()
                .setAppName("OlympicMedals")
                .setMaster("local")
//                .set("spark.driver.host", "localhost")
                .set("spark.executor.instances", "4")
                .set("spark.executor.cores", "4")
                .set("spark.executor.memory", "2g");

        SparkSession sparkSession = SparkSession.builder()
                .config(conf)
                .getOrCreate();

        Dataset<Row> df = sparkSession.read()
                .option("header", "true")
                .option("inferSchema", "true")
                .csv("src/main/resources/data/result.csv");

        System.out.println("about to pivot");

        long before = System.currentTimeMillis();

        df.select("country","sport","year","gold","silver")
                .groupBy("country")
                .pivot("sport", asList("Athletics"))
                .agg(sum("gold").alias("gold"), sum("silver").alias("silver"))
                .show(1000000);

        long after = System.currentTimeMillis();

        System.out.println("first pivot took: " + (after - before) + "ms");


        before = System.currentTimeMillis();

        df.select("country","sport","year","gold","silver")
                .groupBy("country")
                .pivot("sport", asList("Athletics"))
                .agg(sum("gold").alias("gold"), sum("silver").alias("silver"))
                .show(1000000);

        after = System.currentTimeMillis();

        System.out.println("second pivot took: " + (after - before) + "ms");
    }
}