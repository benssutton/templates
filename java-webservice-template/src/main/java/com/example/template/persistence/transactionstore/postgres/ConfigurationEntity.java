package com.example.template.persistence.transactionstore.postgres;

import io.micronaut.data.annotation.Id;
import io.micronaut.data.annotation.MappedEntity;
import io.micronaut.serde.annotation.Serdeable;

@Serdeable
@MappedEntity("configuration")
public record ConfigurationEntity(@Id String key, String value) {}
