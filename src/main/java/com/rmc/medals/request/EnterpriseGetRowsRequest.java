package com.rmc.medals.request;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

public class EnterpriseGetRowsRequest implements Serializable {

    private int startRow;
    private int endRow;

    // row group columns
    private List<ColumnVO> rowGroupCols;

    // value columns
    private List<ColumnVO> valueCols;

    // pivot columns
    private List<ColumnVO> pivotCols;

    // true if pivot mode is one, otherwise false
    private boolean pivotMode;

    // what groups the user is viewing
    private List<String> groupKeys;

    // if filtering, what the filter model is
    private Map<String, FilterModel> filterModel;

    // if sorting, what the sort model is
    private List<SortModel> sortModel;

    public EnterpriseGetRowsRequest() {}

    public EnterpriseGetRowsRequest(int startRow, int endRow, List<ColumnVO> rowGroupCols, List<ColumnVO> valueCols, List<ColumnVO> pivotCols, boolean pivotMode, List<String> groupKeys, Map<String, FilterModel> filterModel, List<SortModel> sortModel) {
        this.startRow = startRow;
        this.endRow = endRow;
        this.rowGroupCols = rowGroupCols;
        this.valueCols = valueCols;
        this.pivotCols = pivotCols;
        this.pivotMode = pivotMode;
        this.groupKeys = groupKeys;
        this.filterModel = filterModel;
        this.sortModel = sortModel;
    }

    public int getStartRow() {
        return startRow;
    }

    public void setStartRow(int startRow) {
        this.startRow = startRow;
    }

    public int getEndRow() {
        return endRow;
    }

    public void setEndRow(int endRow) {
        this.endRow = endRow;
    }

    public List<ColumnVO> getRowGroupCols() {
        return rowGroupCols;
    }

    public void setRowGroupCols(List<ColumnVO> rowGroupCols) {
        this.rowGroupCols = rowGroupCols;
    }

    public List<ColumnVO> getValueCols() {
        return valueCols;
    }

    public void setValueCols(List<ColumnVO> valueCols) {
        this.valueCols = valueCols;
    }

    public List<ColumnVO> getPivotCols() {
        return pivotCols;
    }

    public void setPivotCols(List<ColumnVO> pivotCols) {
        this.pivotCols = pivotCols;
    }

    public boolean isPivotMode() {
        return pivotMode;
    }

    public void setPivotMode(boolean pivotMode) {
        this.pivotMode = pivotMode;
    }

    public List<String> getGroupKeys() {
        return groupKeys;
    }

    public void setGroupKeys(List<String> groupKeys) {
        this.groupKeys = groupKeys;
    }

    public Map<String, FilterModel> getFilterModel() {
        return filterModel;
    }

    public void setFilterModel(Map<String, FilterModel> filterModel) {
        this.filterModel = filterModel;
    }

    public List<SortModel> getSortModel() {
        return sortModel;
    }

    public void setSortModel(List<SortModel> sortModel) {
        this.sortModel = sortModel;
    }
}