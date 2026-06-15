package com.example.template.controller;

import com.example.template.config.AppSettings;
import com.example.template.dto.RootInfo;
import io.micronaut.http.annotation.Controller;
import io.micronaut.http.annotation.Get;

/** Root info page (mirrors the Python main.py get_root): title/version/docs/MCP. */
@Controller("/")
public class RootController {

    private final AppSettings settings;

    public RootController(AppSettings settings) {
        this.settings = settings;
    }

    @Get
    public RootInfo root() {
        return new RootInfo(settings.getAppTitle(), settings.getAppVersion(),
            settings.getAppDescription(), "/swagger-ui", "/mcp");
    }
}
