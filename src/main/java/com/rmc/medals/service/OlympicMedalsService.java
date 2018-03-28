package com.rmc.medals.service;

import com.rmc.medals.filter.ColumnFilter;
import com.rmc.medals.filter.NumberColumnFilter;
import com.rmc.medals.filter.SetColumnFilter;
import com.rmc.medals.request.ColumnVO;
import com.rmc.medals.request.EnterpriseGetRowsRequest;
import com.rmc.medals.request.SortModel;
import com.rmc.medals.response.DataResult;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.spark.sql.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Stream;

import static com.google.common.collect.Streams.zip;
import static com.rmc.medals.util.JsonUtil.asString;
import static java.util.Arrays.copyOfRange;
import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Stream.concat;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.sum;

@Service
public class OlympicMedalsService {

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
    private Map<String, ColumnFilter> filterModel;
    private boolean isGrouping, isPivotMode;

    @Autowired
    private SparkService sparkService;

    public DataResult getData(EnterpriseGetRowsRequest request) {
        rowGroups = request.getRowGroupCols().stream().map(ColumnVO::getField).collect(toList());
        groupKeys = request.getGroupKeys();
        valueColumns = request.getValueCols();
        pivotColumns = request.getPivotCols();
        filterModel = request.getFilterModel();
        sortModel = request.getSortModel();
        isPivotMode = request.isPivotMode();
        isGrouping = rowGroups.size() > groupKeys.size();

        Dataset<Row> df = sparkService.execute(selectSql() + " from medals " + getFilters());

        Dataset<Row> dataFrame = orderBy(groupBy(where(df)));

        return paginate(dataFrame, request.getStartRow(), request.getEndRow());
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

    private Dataset<Row> where(Dataset<Row> df) {
        String whereClause = zip(groupKeys.stream(), rowGroups.stream(),
                (key, group) -> group + " = '" + key + "'")
                .collect(joining(" AND "));

        return groupKeys.isEmpty() ? df : df.filter(whereClause);
    }

    private Dataset<Row> groupBy(Dataset<Row> df) {
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

    private Dataset<Row> orderBy(Dataset<Row> df) {
        Stream<String> groupCols = rowGroups.stream()
                .limit(groupKeys.size() + 1);

        Stream<String> valCols = valueColumns.stream()
                .map(ColumnVO::getField);

        List<String> allCols = concat(groupCols, valCols).collect(toList());

        return df.orderBy(sortModel.stream()
                .map(model -> Pair.of(model.getColId(), model.getSort().equals("asc")))
                .filter(p -> !isGrouping || allCols.contains(p.getKey()))
                .map(p -> p.getValue() ? col(p.getKey()).asc() : col(p.getKey()).desc())
                .toArray(Column[]::new));
    }

    private DataResult paginate(Dataset<Row> df, int startRow, int endRow) {
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

    private String getFilters() {
        Function<Map.Entry<String, ColumnFilter>, String> applyFilters = entry -> {
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

        String filterSql = filterModel.entrySet()
                .stream()
                .map(applyFilters)
                .collect(joining(" AND "));

        return filterSql.isEmpty() ? "" : "WHERE " + filterSql;
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
}