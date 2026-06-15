package com.example.template.service;

import com.example.template.dto.ConfigEntry;
import com.example.template.persistence.transactionstore.postgres.ConfigurationRepository;
import jakarta.inject.Singleton;

import java.util.List;

@Singleton
public class ConfigService {

    private final ConfigurationRepository repository;

    public ConfigService(ConfigurationRepository repository) {
        this.repository = repository;
    }

    public List<ConfigEntry> getAll() {
        return repository.findAllOrdered().stream()
            .map(e -> new ConfigEntry(e.key(), e.value()))
            .toList();
    }

    public ConfigEntry set(String key, String value) {
        repository.upsert(key, value);
        return new ConfigEntry(key, value);
    }
}
