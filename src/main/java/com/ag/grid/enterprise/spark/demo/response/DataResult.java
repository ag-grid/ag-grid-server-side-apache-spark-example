package com.ag.grid.enterprise.spark.demo.response;

import java.util.List;

public class DataResult {
    private List<String> data;
    private int lastRow;
    private List<String> secondaryColumns;

    public DataResult(List<String> data, int lastRow, List<String> secondaryColumns) {
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

    public int getLastRow() {
        return lastRow;
    }

    public void setLastRow(int lastRow) {
        this.lastRow = lastRow;
    }

    public List<String> getSecondaryColumns() {
        return secondaryColumns;
    }

    public void setSecondaryColumns(List<String> secondaryColumns) {
        this.secondaryColumns = secondaryColumns;
    }
}