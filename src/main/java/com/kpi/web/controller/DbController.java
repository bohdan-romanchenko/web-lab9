package com.kpi.web.controller;

import com.kpi.web.service.DbService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping(value = "/api")
public class DbController {

    private final DbService dbService;

    @Autowired
    public DbController(DbService dbService) {
        this.dbService = dbService;
    }

    @RequestMapping(method = RequestMethod.GET)
    public String api() {
        return dbService.run();
    }
}
