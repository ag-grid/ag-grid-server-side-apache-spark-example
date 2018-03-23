package unit;

import com.rmc.medals.builder.SqlQueryBuilder;
import com.rmc.medals.request.ColumnVO;
import com.rmc.medals.request.EnterpriseGetRowsRequest;
import com.rmc.medals.request.FilterModel;
import com.rmc.medals.request.SortModel;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;
import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static org.apache.spark.sql.functions.sum;
import static org.junit.Assert.assertEquals;

public class SqlQueryBuilderTest {

    @Test
    public void fred() {
        SparkConf conf = new SparkConf()
                .setAppName("OlympicMedals")
                .setMaster("local");
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


    @Test
    public void singleRowGroup() {
        EnterpriseGetRowsRequest request = emptyRequest();
        
        request.setRowGroupCols(singletonList(
                new ColumnVO("country", "Country", "country", null)));

        String result = new SqlQueryBuilder().extractQueryFrom(request);
        assertEquals("SELECT country FROM medals GROUP BY country", result);
    }

    @Test
    public void twoRowGroups() {
        EnterpriseGetRowsRequest request = emptyRequest();
        request.setRowGroupCols(asList(
                new ColumnVO("country", "Country", "country", null),
                new ColumnVO("year", "Year", "year", null)));

        assertEquals("SELECT country FROM medals GROUP BY country",
                new SqlQueryBuilder().extractQueryFrom(request));
    }

    @Test
    public void singleGroupExpandedWithOneRowGroup() {
        EnterpriseGetRowsRequest request = emptyRequest();
        request.setGroupKeys(singletonList("Paraguay"));
        request.setRowGroupCols(singletonList(
                new ColumnVO("country", "Country", "country", null)));

        assertEquals("SELECT * FROM medals WHERE country = 'Paraguay' ",
                new SqlQueryBuilder().extractQueryFrom(request));
    }

    @Test
    public void singleGroupExpandedWithTwoRowGroups() {
        EnterpriseGetRowsRequest request = emptyRequest();
        request.setGroupKeys(singletonList("Paraguay"));
        request.setRowGroupCols(asList(
                new ColumnVO("country", "Country", "country", null),
                new ColumnVO("year", "Year", "year", null)));

        assertEquals("SELECT country, year FROM medals WHERE country = 'Paraguay' GROUP BY country, year",
                new SqlQueryBuilder().extractQueryFrom(request));
    }

    @Test
    public void twoGroupExpandedWithTwoRowGroups() {
        EnterpriseGetRowsRequest request = emptyRequest();
        request.setGroupKeys(asList("South Korea", "2008"));
        request.setRowGroupCols(asList(
                new ColumnVO("country", "Country", "country", null),
                new ColumnVO("year", "Year", "year", null)));

        assertEquals("SELECT * FROM medals WHERE country = 'South Korea' AND year = '2008' ",
                new SqlQueryBuilder().extractQueryFrom(request));
    }

    @Test
    public void singleGroupExpandedWithOneRowGroupWithSorting() {
        EnterpriseGetRowsRequest request = emptyRequest();
        request.setGroupKeys(singletonList("Paraguay"));
        request.setRowGroupCols(singletonList(
                new ColumnVO("country", "Country", "country", null)));
        request.setSortModel(asList(
                new SortModel("athlete", "asc"),
                new SortModel("year", "desc")));

        assertEquals("SELECT * FROM medals WHERE country = 'Paraguay'  ORDER BY athlete asc, year desc",
                new SqlQueryBuilder().extractQueryFrom(request));
    }

    @Test
    public void singleRowGroupWithFilter() {
        EnterpriseGetRowsRequest request = emptyRequest();

        Map<String, FilterModel> mymap = new HashMap<>();
        mymap.put("age", new FilterModel("equals", "22", "number"));

        request.setFilterModel(mymap);
        request.setRowGroupCols(singletonList(
                new ColumnVO("country", "Country", "country", null)));

        String result = new SqlQueryBuilder().extractQueryFrom(request);
        assertEquals("SELECT country FROM medals WHERE age = 22 GROUP BY country", result);
    }

    private EnterpriseGetRowsRequest emptyRequest() {
        return new EnterpriseGetRowsRequest(0, 0, emptyList(), emptyList(), emptyList(), false, emptyList(), emptyMap(), emptyList());
    }
}