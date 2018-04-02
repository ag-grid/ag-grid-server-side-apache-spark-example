package com.ag.grid.enterprise.spark.demo.controller;

import com.ag.grid.enterprise.spark.demo.dao.OlympicMedalDao;
import com.ag.grid.enterprise.spark.demo.request.EnterpriseGetRowsRequest;
import com.ag.grid.enterprise.spark.demo.response.DataResult;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class OlympicMedalsController {

    @Autowired
    private OlympicMedalDao medalDao;

    @RequestMapping(method = RequestMethod.POST, value = "/getRows")
    public ResponseEntity<String> getRows(@RequestBody EnterpriseGetRowsRequest request) {
        DataResult data = medalDao.getData(request);
        return new ResponseEntity<>(asJsonResponse(data), HttpStatus.OK);
    }

    private String asJsonResponse(DataResult result) {
        String secondaryColumns = result.getSecondaryColumns().isEmpty() ? "" :
                "\"" + String.join("\", \"", result.getSecondaryColumns()) + "\"";

        return "{" +
                "\"data\": [" + String.join(",", result.getData()) + "], " +
                "\"lastRow\":" + result.getLastRow() + ", " +
                "\"secondaryColumns\": [" + secondaryColumns + "] " +
                "}";
    }
}