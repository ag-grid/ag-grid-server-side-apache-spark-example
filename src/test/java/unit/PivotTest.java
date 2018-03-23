package unit;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.Test;

import static java.util.Arrays.asList;
import static org.apache.spark.sql.functions.sum;

public class PivotTest {

    @Test
    public void pivotTest1() {
        SparkConf conf = new SparkConf()
                .setAppName("OlympicMedals")
                .setMaster("local")
                .set("spark.executor.instances", "20")
                .set("spark.executor.cores", "8")
                .set("spark.cores.max", "10")
                .set("spark.executor.memory", "2g")
                .set("spark.sql.shuffle.partitions", "5");
//                .set("spark.driver.host", "localhost");

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
                .pivot("sport")
                .agg(sum("gold").alias("gold"), sum("silver").alias("silver"));

        long after = System.currentTimeMillis();

        System.out.println("first pivot took: " + (after - before) + "ms");


        before = System.currentTimeMillis();

        df.select("country","sport","year","gold","silver")
                .groupBy("country")
                .pivot("sport")
                .agg(sum("gold").alias("gold"), sum("silver").alias("silver"));

        after = System.currentTimeMillis();

        System.out.println("second pivot took: " + (after - before) + "ms");
    }


    @Test
    public void pivotTest2() {
        SparkConf conf = new SparkConf()
                .setAppName("OlympicMedals")
                .setMaster("local")
                .set("spark.executor.instances", "20")
                .set("spark.executor.cores", "8")
                .set("spark.executor.memory", "2g");

//                .set("spark.driver.host", "localhost");

        SparkSession sparkSession = SparkSession.builder()
                .config(conf)

                .getOrCreate();

        Dataset<Row> df = sparkSession.read()
                .option("header", "true")
                .option("inferSchema", "true")
                .csv("src/main/resources/data/result.csv");

//        df.createOrReplaceTempView("medals");

        System.out.println("about to pivot");

        long before = System.currentTimeMillis();

//        df.select("country","sport","year","gold","silver")
//                .groupBy("country")
//                .pivot("sport", asList("Athletics"))
//                .agg(sum("gold").alias("gold"), sum("silver").alias("silver"))
//                .show(1000000);

        df.select("country, year, gold, silver")
                .groupBy("country")
                .pivot("year")
                .agg(sum("gold"), sum("silver"))
                .show(10);

//        (sql("""select *, concat('Q', d_qoy) as qoy
//                from store_sales
//                join date_dim on ss_sold_date_sk = d_date_sk
//                join item on ss_item_sk = i_item_sk""")
//                        .groupBy("i_category")
//                        .pivot("qoy")
//                        .agg(round(sum("ss_sales_price")/1000000,2))
//                        .show)
/*
        long after = System.currentTimeMillis();

        System.out.println("first pivot took: " + (after - before) + "ms");


        before = System.currentTimeMillis();

        df.select("country, year, gold, silver")
                .groupBy("country")
                .pivot("year")
                .agg(sum("gold"), sum("silver"))
                .show(10);

        after = System.currentTimeMillis();

        System.out.println("second pivot took: " + (after - before) + "ms");

        before = System.currentTimeMillis();

        df.select("country, year, gold, silver")
                .groupBy("country")
                .pivot("year")
                .agg(sum("gold"), sum("silver"))
                .show(1000);

        after = System.currentTimeMillis();

        System.out.println("third pivot took: " + (after - before) + "ms");
*/
    }
}