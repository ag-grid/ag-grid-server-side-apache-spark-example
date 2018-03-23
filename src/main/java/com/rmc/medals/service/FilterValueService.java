package com.rmc.medals.service;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;

import static java.util.stream.Collectors.toList;

@Service
public class FilterValueService {

    @Autowired
    private SparkService sparkService;

    public List<String> getAthletes() {
        return getStringFilterValues("SELECT DISTINCT athlete FROM medals");
    }

    public List<String> getSports() {
        return getStringFilterValues("SELECT DISTINCT sport FROM medals");
    }

    public List<Integer> getYears() {
        return sparkService
                .execute("SELECT DISTINCT year FROM medals")
                .collectAsList()
                .stream()
                .map(row -> row.getInt(0))
                .collect(toList());
    }

    private List<String> getStringFilterValues(String query) {
        return sparkService
                .execute(query)
                .collectAsList()
                .stream()
                .map(row -> row.getString(0))
                .collect(toList());
    }
}