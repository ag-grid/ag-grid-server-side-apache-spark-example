package com.ag.grid.enterprise.spark.demo.util;

import com.ag.grid.enterprise.spark.demo.response.DataResult;

import java.util.Arrays;
import java.util.List;
import java.util.function.Function;

import static java.util.stream.Collectors.joining;

public class JsonUtil {

    public static String asJsonResponse(DataResult result) {
        String secondaryColumns = result.getSecondaryColumns().isEmpty() ? "" :
                "\"" + String.join("\", \"", result.getSecondaryColumns()) + "\"";

        return "{" +
            "\"data\": [" + String.join(",", result.getData()) + "], " +
            "\"lastRow\":" + result.getLastRow() + ", " +
            "\"secondaryColumns\": [" + secondaryColumns + "] " +
        "}";
    }

    public static String asJsonArr(List<?> l) {
        Function<Object, String> addQuotes = s -> "\"" + s + "\"";
        return "[" + l.stream().map(addQuotes).collect(joining(", ")) + "]";
    }

    public static String asString(List<String> l) {
        Function<String, String> addQuotes = s -> "\"" + s + "\"";
        return "(" + l.stream().map(addQuotes).collect(joining(", ")) + ")";
    }

    public static String asString(String[] arr) {
        Function<String, String> addQuotes = s -> "\"" + s + "\"";
        return "(" + Arrays.stream(arr).map(addQuotes).collect(joining(", ")) + ")";
    }
}