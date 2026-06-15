package com.example.template.controller;

import com.example.template.dto.CacheEntry;
import com.example.template.dto.CacheSetRequest;
import com.example.template.service.CacheService;
import io.micronaut.http.HttpResponse;
import io.micronaut.http.HttpStatus;
import io.micronaut.http.annotation.Body;
import io.micronaut.http.annotation.Controller;
import io.micronaut.http.annotation.Get;
import io.micronaut.http.annotation.PathVariable;
import io.micronaut.http.annotation.Post;
import io.micronaut.http.exceptions.HttpStatusException;
import io.micronaut.scheduling.TaskExecutors;
import io.micronaut.scheduling.annotation.ExecuteOn;
import jakarta.validation.Valid;

/** Mirrors Python routers/cache.py: POST -> 201, GET -> 404 when absent. */
@Controller("/cache")
@ExecuteOn(TaskExecutors.BLOCKING)
public class CacheController {

    private final CacheService cacheService;

    public CacheController(CacheService cacheService) {
        this.cacheService = cacheService;
    }

    @Post
    public HttpResponse<CacheEntry> set(@Valid @Body CacheSetRequest body) {
        return HttpResponse.created(cacheService.set(body.key(), body.value(), body.ttlSeconds()));
    }

    @Get("/{key}")
    public CacheEntry get(@PathVariable String key) {
        CacheEntry entry = cacheService.get(key);
        if (entry == null) {
            throw new HttpStatusException(HttpStatus.NOT_FOUND, "Key '" + key + "' not found");
        }
        return entry;
    }
}
