package com.rmc.medals.controller;

import com.rmc.medals.model.Test;
import com.rmc.medals.request.EnterpriseGetRowsRequest;
import com.rmc.medals.response.DataResult;
import com.rmc.medals.service.FilterValueService;
import com.rmc.medals.service.OlympicMedalsService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.Map;

import static com.rmc.medals.util.JsonUtil.asJsonArr;
import static com.rmc.medals.util.JsonUtil.asJsonResponse;

@RestController
public class OlympicMedalsController {

    @Autowired
    JdbcTemplate template;

    @Autowired
    private OlympicMedalsService medalsService;

    @Autowired
    private FilterValueService filterValueService;

    @RequestMapping(method = RequestMethod.POST, value = "/olympic-medals/getData")
    public ResponseEntity<String> getData(@RequestBody EnterpriseGetRowsRequest request) {

        long before = System.currentTimeMillis();
        DataResult data = medalsService.getData(request);
        long after = System.currentTimeMillis();

        System.out.println(">>>> getData() took " + (after-before) + "ms");

        return new ResponseEntity<>(asJsonResponse(data), HttpStatus.OK);
    }

    @RequestMapping(method = RequestMethod.GET, value = "/olympic-medals/getAll")
    public ResponseEntity getAllItems(){

        List<Map<String, Object>> maps = template.queryForList("select * from test");

        List<Test> items = template.query(
                "select name from test",
                (result, rowNum) -> new Test(result.getString("name"))
        );

        return new ResponseEntity(items, HttpStatus.OK);
    }

    @RequestMapping(method = RequestMethod.GET, value = "/olympic-medals/getAthletes")
    public ResponseEntity<String> getAthletes() {
        List<String> athletes = filterValueService.getAthletes();
        return new ResponseEntity<>(asJsonArr(athletes), HttpStatus.OK);
    }

    @RequestMapping(method = RequestMethod.GET, value = "/olympic-medals/getSports")
    public ResponseEntity<String> getSports() {
        List<String> sports = filterValueService.getSports();
        return new ResponseEntity<>(asJsonArr(sports), HttpStatus.OK);
    }

    @RequestMapping(method = RequestMethod.GET, value = "/olympic-medals/getYears")
    public ResponseEntity<String> getYears() {
        List<Integer> years = filterValueService.getYears();
        return new ResponseEntity<>(asJsonArr(years), HttpStatus.OK);
    }
}