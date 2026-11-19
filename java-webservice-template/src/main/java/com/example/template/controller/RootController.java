package com.example.template.controller;

import com.example.template.config.AppSettings;
import com.example.template.dto.RootInfo;
import io.micronaut.http.annotation.Controller;
import io.micronaut.http.annotation.Get;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;

/** Root info page (mirrors the Python main.py get_root): title/version/docs/MCP. */
@Controller("/")
@Tag(name = "root")
public class RootController {

    private final AppSettings settings;

    public RootController(AppSettings settings) {
        this.settings = settings;
    }

    @Get
    @Operation(summary = "Service title, version and links to the docs and MCP endpoint")
    public RootInfo root() {
        return new RootInfo(
                settings.appTitle(),
                settings.appVersion(),
                settings.appDescription(),
                "/swagger-ui",
                "/mcp");
    }
}
