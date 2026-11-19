package com.example.template.mcp;

import com.example.template.config.AppSettings;
import io.micronaut.context.event.BeanCreatedEvent;
import io.micronaut.context.event.BeanCreatedEventListener;
import io.modelcontextprotocol.server.McpServer;
import jakarta.inject.Singleton;

/**
 * Adds {@code template.mcp-instructions} to the MCP server spec built by micronaut-mcp, which
 * configures only the server name and version.
 */
@Singleton
class McpInstructions implements BeanCreatedEventListener<McpServer.StatelessSyncSpecification> {

    private final AppSettings settings;

    McpInstructions(AppSettings settings) {
        this.settings = settings;
    }

    @Override
    public McpServer.StatelessSyncSpecification onCreated(
            BeanCreatedEvent<McpServer.StatelessSyncSpecification> event) {
        return event.getBean().instructions(settings.mcpInstructions());
    }
}
