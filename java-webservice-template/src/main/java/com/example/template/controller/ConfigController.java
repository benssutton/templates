package com.example.template.controller;

import com.example.template.dto.ConfigEntry;
import com.example.template.dto.ConfigSetRequest;
import com.example.template.service.ConfigService;
import io.micronaut.http.HttpResponse;
import io.micronaut.http.annotation.Body;
import io.micronaut.http.annotation.Controller;
import io.micronaut.http.annotation.Get;
import io.micronaut.http.annotation.Post;
import io.micronaut.scheduling.TaskExecutors;
import io.micronaut.scheduling.annotation.ExecuteOn;
import jakarta.validation.Valid;

import java.util.List;

@Controller("/config")
@ExecuteOn(TaskExecutors.BLOCKING)
public class ConfigController {

    private final ConfigService configService;

    public ConfigController(ConfigService configService) {
        this.configService = configService;
    }

    @Post
    public HttpResponse<ConfigEntry> set(@Valid @Body ConfigSetRequest body) {
        return HttpResponse.created(configService.set(body.key(), body.value()));
    }

    @Get
    public List<ConfigEntry> getAll() {
        return configService.getAll();
    }
}
