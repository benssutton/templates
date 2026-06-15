package com.example.template.persistence.analyticsstore.clickhouse;

import io.micronaut.data.annotation.Id;
import io.micronaut.data.annotation.MappedEntity;
import io.micronaut.serde.annotation.Serdeable;

@Serdeable
@MappedEntity("items")
public record ItemEntity(@Id long id, String name, String value) {}
