package com.rmc.medals.builder;

import com.rmc.medals.request.ColumnVO;
import com.rmc.medals.request.EnterpriseGetRowsRequest;
import com.rmc.medals.request.FilterModel;
import com.rmc.medals.request.SortModel;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Stream;

import static com.google.common.collect.Streams.zip;
import static com.rmc.medals.util.JsonUtil.asString;
import static java.lang.String.join;
import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Stream.concat;

public class SqlQueryBuilder {

    private static Map<String, String> operatorMap = new HashMap<String, String>() {{
        put("equals", "=");
        put("notEqual", "<>");
        put("lessThan", "<");
        put("lessThanOrEqual", "<=");
        put("greaterThan", ">");
        put("greaterThanOrEqual", ">=");
    }};

    public String extractQueryFrom(EnterpriseGetRowsRequest request) {
        List<String> rowGroups = request.getRowGroupCols().stream().map(ColumnVO::getField).collect(toList());
        List<String> groupKeys = request.getGroupKeys();
        List<ColumnVO> valueColumns = request.getValueCols();
        Map<String, FilterModel> filterModel = request.getFilterModel();

        boolean isGrouping = rowGroups.size() > groupKeys.size();

        // SELECT
        List<String> rowGroupsToInclude = rowGroups.stream().limit(groupKeys.size() + 1).collect(toList());
        Stream<String> valueCols = valueColumns.stream().map(valueCol -> valueCol.getAggFunc() + '(' + valueCol.getField() + ") as " + valueCol.getField());
        List<String> selectCols = concat(rowGroupsToInclude.stream(), valueCols).collect(toList());
        String select = isGrouping ? "SELECT " + join(", ", selectCols) : "SELECT *";

        // WHERE
        List<String> groups =
                zip(groupKeys.stream(), rowGroups.stream(),
                        (key, group) -> group + " = '" + key + "'")
                        .collect(toList());

        String SQL = "";
        if (!groups.isEmpty()) {
            SQL = "WHERE " + join(" AND ", groups) + " ";
        }

        String filters = getFilters(filterModel);
        if (!filters.isEmpty()) {
            SQL += (groups.isEmpty() ? "WHERE " : "AND ") + filters + " ";
        }

        if (isGrouping) {
            SQL += "GROUP BY " + join(", ", rowGroupsToInclude);
        }

        String where = SQL;

        return select + " FROM medals " + where + getOrderBy(request);
    }

    private String getFilters(Map<String, FilterModel> filterModel) {
        Predicate<FilterModel> isSetFilter = fm -> fm.getFilterType().equals("set");
        Predicate<FilterModel> isInRangeFilter = fm -> fm.getType() != null && fm.getType().equals("inRange");

        Function<FilterModel, String> setFilter = fm -> fm.getValues().isEmpty() ? " IN ('') " : " IN " + asString(fm.getValues());
        Function<FilterModel, String> inRangeFilter = fm -> " BETWEEN " + fm.getFilter() + " AND " + fm.getFilterTo();
        Function<FilterModel, String> regularFilter = fm -> " " + operatorMap.get(fm.getType()) + " " + fm.getFilter();

        Function<Map.Entry<String, FilterModel>, String> filters = entry -> entry.getKey() +
                (isSetFilter.test(entry.getValue()) ? setFilter.apply(entry.getValue()) :
                        isInRangeFilter.test(entry.getValue()) ? inRangeFilter.apply(entry.getValue()) :
                                regularFilter.apply(entry.getValue()));
        return filterModel
                .entrySet()
                .stream()
                .map(filters)
                .collect(joining(" AND "));
    }

    private String getOrderBy(EnterpriseGetRowsRequest request) {
        List<String> rowGroups = getRowGroups(request);
        boolean doingGrouping = isDoingGrouping(request);

        Function<SortModel, String> orderByMapper = model -> model.getColId() + " " + model.getSort();

        int num = doingGrouping ? request.getGroupKeys().size() + 1 : 10;

        String res = request.getSortModel()
                .stream()
                .filter(model -> !doingGrouping || rowGroups.contains(model.getColId()))
                .map(orderByMapper)
                .limit(num)
                .collect(joining(", "));

        return res.isEmpty() ? "" : " ORDER BY " + res;
    }

    private boolean isDoingGrouping(EnterpriseGetRowsRequest request) {
        return request.getRowGroupCols().size() > request.getGroupKeys().size();
    }

    private List<String> getRowGroups(EnterpriseGetRowsRequest request) {
        return request.getRowGroupCols().stream()
                .map(ColumnVO::getField)
                .collect(toList());
    }
}