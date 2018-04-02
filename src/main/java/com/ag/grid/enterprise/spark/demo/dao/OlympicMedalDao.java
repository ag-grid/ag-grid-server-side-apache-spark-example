package com.ag.grid.enterprise.spark.demo.dao;

import com.ag.grid.enterprise.spark.demo.filter.ColumnFilter;
import com.ag.grid.enterprise.spark.demo.filter.NumberColumnFilter;
import com.ag.grid.enterprise.spark.demo.filter.SetColumnFilter;
import com.ag.grid.enterprise.spark.demo.request.ColumnVO;
import com.ag.grid.enterprise.spark.demo.request.EnterpriseGetRowsRequest;
import com.ag.grid.enterprise.spark.demo.request.SortModel;
import com.ag.grid.enterprise.spark.demo.response.DataResult;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.StructType;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Stream;

import static com.google.common.collect.Streams.zip;
import static java.util.Arrays.copyOfRange;
import static java.util.Arrays.stream;
import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Stream.concat;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.sum;

@Service
public class OlympicMedalDao {
    private List<String> rowGroups, groupKeys;
    private List<ColumnVO> valueColumns, pivotColumns;
    private List<SortModel> sortModel;
    private Map<String, ColumnFilter> filterModel;
    private boolean isGrouping, isPivotMode;

    private SparkSession sparkSession;

    @PostConstruct
    public void init() {
        SparkConf sparkConf = new SparkConf()
                .setAppName("OlympicMedals")
                .setMaster("local[*]")
                .set("spark.sql.shuffle.partitions", "1");

        this.sparkSession = SparkSession.builder()
                .config(sparkConf)
                .getOrCreate();

        Dataset<Row> dataFrame = this.sparkSession.read()
                .option("header", "true")
                .option("inferSchema", "true")
                .csv("src/main/resources/data/result.csv");

        dataFrame.cache();

        dataFrame.createOrReplaceTempView("medals");
    }

    public DataResult getData(EnterpriseGetRowsRequest request) {
        rowGroups = request.getRowGroupCols().stream().map(ColumnVO::getField).collect(toList());
        groupKeys = request.getGroupKeys();
        valueColumns = request.getValueCols();
        pivotColumns = request.getPivotCols();
        filterModel = request.getFilterModel();
        sortModel = request.getSortModel();
        isPivotMode = request.isPivotMode();
        isGrouping = rowGroups.size() > groupKeys.size();

        Dataset<Row> df = sparkSession.sql(selectSql() + " from medals ");

        Dataset<Row> results = orderBy(groupBy(filter(df)));

        return paginate(results, request.getStartRow(), request.getEndRow());
    }

    private String selectSql() {
        if (!isGrouping) return "select *";

        Stream<String> groupCols = rowGroups.stream()
                .limit(groupKeys.size() + 1);

        Stream<String> valCols = valueColumns.stream()
                .map(ColumnVO::getField);

        Stream<String> pivotCols = isPivotMode ?
                pivotColumns.stream().map(ColumnVO::getField) : Stream.empty();

        return "select " + concat(groupCols, concat(pivotCols, valCols)).collect(joining(","));
    }

    private Dataset<Row> groupBy(Dataset<Row> df) {
        if (!isGrouping) return df;

        Column[] groups = rowGroups.stream()
                .limit(groupKeys.size() + 1)
                .map(functions::col)
                .toArray(Column[]::new);

        return agg(pivot(df.groupBy(groups)));
    }

    private RelationalGroupedDataset pivot(RelationalGroupedDataset groupedDf) {
        if (!isPivotMode) return groupedDf;

        // spark sql only supports a single pivot column
        Optional<String> pivotColumn = pivotColumns.stream()
                .map(ColumnVO::getField)
                .findFirst();

        return pivotColumn.map(groupedDf::pivot).orElse(groupedDf);
    }

    private Dataset<Row> agg(RelationalGroupedDataset groupedDf) {
        if (valueColumns.isEmpty()) return groupedDf.count();

        Column[] aggCols = valueColumns
                .stream()
                .map(ColumnVO::getField)
                .map(field -> sum(field).alias(field))
                .toArray(Column[]::new);

        return groupedDf.agg(aggCols[0], copyOfRange(aggCols, 1, aggCols.length));
    }

    private Dataset<Row> orderBy(Dataset<Row> df) {
        Stream<String> groupCols = rowGroups.stream()
                .limit(groupKeys.size() + 1);

        Stream<String> valCols = valueColumns.stream()
                .map(ColumnVO::getField);

        List<String> allCols = concat(groupCols, valCols).collect(toList());

        Column[] cols = sortModel.stream()
                .map(model -> Pair.of(model.getColId(), model.getSort().equals("asc")))
                .filter(p -> !isGrouping || allCols.contains(p.getKey()))
                .map(p -> p.getValue() ? col(p.getKey()).asc() : col(p.getKey()).desc())
                .toArray(Column[]::new);

        return df.orderBy(cols);
    }

    private Dataset<Row> filter(Dataset<Row> df) {
        Function<Map.Entry<String, ColumnFilter>, String> applyColumnFilters = entry -> {
            String columnName = entry.getKey();
            ColumnFilter filter = entry.getValue();

            if (filter instanceof SetColumnFilter) {
                return setFilter().apply(columnName, (SetColumnFilter) filter);
            }

            if (filter instanceof NumberColumnFilter) {
                return numberFilter().apply(columnName, (NumberColumnFilter) filter);
            }

            return "";
        };

        Stream<String> columnFilters = filterModel.entrySet()
                .stream()
                .map(applyColumnFilters);

        Stream<String> groupToFilter = zip(groupKeys.stream(), rowGroups.stream(),
                (key, group) -> group + " = '" + key + "'");

        String filters = concat(columnFilters, groupToFilter)
                .collect(joining(" AND "));

        return filters.isEmpty() ? df : df.filter(filters);
    }

    private BiFunction<String, SetColumnFilter, String> setFilter() {
        return (String columnName, SetColumnFilter filter) ->
                columnName + (filter.getValues().isEmpty() ? " IN ('') " : " IN " + asString(filter.getValues()));
    }

    private BiFunction<String, NumberColumnFilter, String> numberFilter() {
        return (String columnName, NumberColumnFilter filter) -> {
            Integer filterValue = filter.getFilter();
            String filerType = filter.getType();
            String operator = operatorMap.get(filerType);

            return columnName + (filerType.equals("inRange") ?
                    " BETWEEN " + filterValue + " AND " + filter.getFilterTo() : " " + operator + " " + filterValue);
        };
    }

    private DataResult paginate(Dataset<Row> df, int startRow, int endRow) {
        // save schema to recreate data frame
        StructType schema = df.schema();

        // obtain row count
        long rowCount = df.count();

        // convert data frame to RDD and introduce a row index so we can filter results by range
        JavaPairRDD<Row, Long> zippedRows = df.toJavaRDD().zipWithIndex();

        // filter rows by row index using the requested range (startRow, endRow), this ensures we don't run out of memory
        JavaRDD<Row> filteredRdd =
                zippedRows.filter(pair -> pair._2 >= startRow && pair._2 <= endRow).map(pair -> pair._1);

        // collect paginated results into a list of json objects
        List<String> paginatedResults = sparkSession.sqlContext()
                .createDataFrame(filteredRdd, schema)
                .toJSON()
                .collectAsList();

        // calculate last row
        long lastRow = endRow >= rowCount ? rowCount : -1;

        return new DataResult(paginatedResults, lastRow, getSecondaryColumns(df));
    }

    private List<String> getSecondaryColumns(Dataset<Row> df) {
        return stream(df.schema().fieldNames())
                .filter(f -> !rowGroups.contains(f)) // filter out group fields
                .collect(toList());
    }

    private String asString(List<String> l) {
        Function<String, String> addQuotes = s -> "\"" + s + "\"";
        return "(" + l.stream().map(addQuotes).collect(joining(", ")) + ")";
    }

    private static Map<String, String> operatorMap = new HashMap<String, String>() {{
        put("equals", "=");
        put("notEqual", "<>");
        put("lessThan", "<");
        put("lessThanOrEqual", "<=");
        put("greaterThan", ">");
        put("greaterThanOrEqual", ">=");
    }};
}