package com.rmc.medals.builder;

import com.rmc.medals.request.ColumnVO;
import com.rmc.medals.request.EnterpriseGetRowsRequest;
import com.rmc.medals.request.FilterModel;
import com.rmc.medals.request.SortModel;
import com.rmc.medals.response.DataResult;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.spark.sql.*;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Stream;

import static com.google.common.collect.Streams.zip;
import static com.rmc.medals.util.JsonUtil.asString;
import static java.util.Arrays.copyOfRange;
import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Stream.concat;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.sum;

public class DataFrameBuilder {

    private static Map<String, String> operatorMap = new HashMap<String, String>() {{
        put("equals", "=");
        put("notEqual", "<>");
        put("lessThan", "<");
        put("lessThanOrEqual", "<=");
        put("greaterThan", ">");
        put("greaterThanOrEqual", ">=");
    }};

    private List<String> rowGroups, groupKeys;
    private List<ColumnVO> valueColumns, pivotColumns;
    private List<SortModel> sortModel;
    private Map<String, FilterModel> filterModel;
    private boolean isGrouping, isPivotMode;


    public DataFrameBuilder(EnterpriseGetRowsRequest request) {
        rowGroups = request.getRowGroupCols().stream().map(ColumnVO::getField).collect(toList());
        groupKeys = request.getGroupKeys();
        valueColumns = request.getValueCols();
        pivotColumns = request.getPivotCols();
        filterModel = request.getFilterModel();
        sortModel = request.getSortModel();
        isPivotMode = request.isPivotMode();
        isGrouping = rowGroups.size() > groupKeys.size();
    }

    public String selectSql() {
        if (!isGrouping) return "SELECT *";

        Stream<String> groupCols = rowGroups.stream()
                .limit(groupKeys.size() + 1);

        Stream<String> valCols = valueColumns.stream()
                .map(ColumnVO::getField);

        Stream<String> pivotCols = isPivotMode ?
                pivotColumns.stream().map(ColumnVO::getField) : Stream.empty();

        return "SELECT " + concat(groupCols, concat(pivotCols, valCols)).collect(joining(","));
    }

    public Dataset<Row> where(Dataset<Row> df) {
        String whereClause = zip(groupKeys.stream(), rowGroups.stream(),
                (key, group) -> group + " = '" + key + "'")
                .collect(joining(" AND "));

        return groupKeys.isEmpty() ? df : df.filter(whereClause);
    }

    public Dataset<Row> groupBy(Dataset<Row> df) {
        if (!isGrouping) return df;

        Column[] groups = rowGroups.stream()
                .limit(groupKeys.size() + 1)
                .map(functions::col)
                .toArray(Column[]::new);

        return agg(pivot(df.groupBy(groups)));
    }

    private RelationalGroupedDataset pivot(RelationalGroupedDataset df) {
        if (!isPivotMode || pivotColumns.isEmpty()) return df;

        List<String> pivots = pivotColumns.stream()
                .map(ColumnVO::getField)
                .collect(toList());

        return df.pivot(pivots.get(0));
    }

    private Dataset<Row> agg(RelationalGroupedDataset df) {
        if (valueColumns.isEmpty()) return df.count();

        Column[] aggCols = valueColumns
                .stream()
                .map(ColumnVO::getField)
                .map(field -> sum(field).alias(field))
                .toArray(Column[]::new);

        return df.agg(aggCols[0], copyOfRange(aggCols, 1, aggCols.length));
    }

    public Dataset<Row> orderBy(Dataset<Row> df) {
        Stream<String> groupCols = rowGroups.stream()
                .limit(groupKeys.size() + 1);

        Stream<String> valCols = valueColumns.stream()
                .map(ColumnVO::getField);

        List<String> allCols = concat(groupCols, valCols).collect(toList());

        return df.orderBy(sortModel.stream()
                .map(model -> Pair.of(model.getColId(), model.getSort().equals("ASC")))
                .filter(p -> !isGrouping || allCols.contains(p.getKey()))
                .map(p -> p.getValue() ? col(p.getKey()).asc() : col(p.getKey()).desc())
                .toArray(Column[]::new));
    }

    public DataResult paginate(Dataset<Row> df, int startRow, int endRow) {
        List<String> allResults = df.toJSON().collectAsList();

        List<String> paginatedResults = allResults.stream()
                .skip(startRow)
                .limit(endRow - startRow)
                .collect(toList());

        int lastRow = endRow >= allResults.size() ? allResults.size() : -1;

        return new DataResult(paginatedResults, lastRow, getSecondaryColumns(df));
    }

    private List<String> getSecondaryColumns(Dataset<Row> df) {
        return Arrays.stream(df.schema().fieldNames())
                .filter(f -> !rowGroups.contains(f)) // filter out group fields
                .collect(toList());
    }

    public String getFilters() {
        Predicate<FilterModel> isSetFilter = fm -> fm.getFilterType().equals("set");
        Predicate<FilterModel> isInRangeFilter = fm -> fm.getType() != null && fm.getType().equals("inRange");

        Function<FilterModel, String> setFilter = fm ->
                fm.getValues().isEmpty() ? " IN ('') " : " IN " + asString(fm.getValues());

        Function<FilterModel, String> inRangeFilter = fm -> " BETWEEN " + fm.getFilter() + " AND " + fm.getFilterTo();
        Function<FilterModel, String> regularFilter = fm -> " " + operatorMap.get(fm.getType()) + " " + fm.getFilter();

        Function<Map.Entry<String, FilterModel>, String> filters = entry -> entry.getKey() +
                (isSetFilter.test(entry.getValue()) ? setFilter.apply(entry.getValue()) :
                        isInRangeFilter.test(entry.getValue()) ? inRangeFilter.apply(entry.getValue()) :
                                regularFilter.apply(entry.getValue()));

        String filterSql = filterModel
                .entrySet()
                .stream()
                .map(filters)
                .collect(joining(" AND "));

        return filterSql.isEmpty() ? "" : "WHERE " + filterSql;
    }

}
