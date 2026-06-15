package com.example.template.health;

import com.example.template.dto.health.IngestHealth;

public interface IngestHealthProvider {
    IngestHealth currentHealth();
}
