package com.ag.grid.enterprise.spark.demo.controller;

import com.ag.grid.enterprise.spark.demo.request.EnterpriseGetRowsRequest;
import com.ag.grid.enterprise.spark.demo.response.DataResult;
import com.ag.grid.enterprise.spark.demo.service.FilterValueService;
import com.ag.grid.enterprise.spark.demo.service.OlympicMedalsService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

import static com.ag.grid.enterprise.spark.demo.util.JsonUtil.asJsonArr;
import static com.ag.grid.enterprise.spark.demo.util.JsonUtil.asJsonResponse;

@RestController
public class OlympicMedalsController {

    @Autowired
    private OlympicMedalsService medalsService;

    @Autowired
    private FilterValueService filterValueService;

    @RequestMapping(method = RequestMethod.POST, value = "/olympic-medals/getData")
    public ResponseEntity<String> getData(@RequestBody EnterpriseGetRowsRequest request) {

        DataResult data = medalsService.getData(request);

        return new ResponseEntity<>(asJsonResponse(data), HttpStatus.OK);
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