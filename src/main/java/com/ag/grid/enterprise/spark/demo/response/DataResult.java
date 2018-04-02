package com.ag.grid.enterprise.spark.demo.response;

import java.util.List;

public class DataResult {
    private List<String> data;
    private long lastRow;
    private List<String> secondaryColumns;

    public DataResult(List<String> data, long lastRow, List<String> secondaryColumns) {
        this.data = data;
        this.lastRow = lastRow;
        this.secondaryColumns = secondaryColumns;
    }

    public List<String> getData() {
        return data;
    }

    public void setData(List<String> data) {
        this.data = data;
    }

    public long getLastRow() {
        return lastRow;
    }

    public void setLastRow(long lastRow) {
        this.lastRow = lastRow;
    }

    public List<String> getSecondaryColumns() {
        return secondaryColumns;
    }

    public void setSecondaryColumns(List<String> secondaryColumns) {
        this.secondaryColumns = secondaryColumns;
    }
}