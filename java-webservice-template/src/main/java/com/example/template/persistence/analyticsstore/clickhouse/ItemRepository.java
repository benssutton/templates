package com.example.template.persistence.analyticsstore.clickhouse;

import io.micronaut.data.annotation.Query;
import io.micronaut.data.annotation.Repository;
import io.micronaut.data.jdbc.annotation.JdbcRepository;
import io.micronaut.data.model.query.builder.sql.Dialect;
import io.micronaut.data.repository.GenericRepository;

import java.util.List;

/**
 * ClickHouse analytics reads (mirrors Python services/data.py). Bound to the
 * {@code clickhouse} datasource; native {@code @Query} since ClickHouse is not a
 * derived-query dialect.
 */
@JdbcRepository(dialect = Dialect.ANSI)
@Repository("clickhouse")
public interface ItemRepository extends GenericRepository<ItemEntity, Long> {

    @Query("SELECT count() FROM items")
    long countAll();

    @Query("SELECT id, name, value FROM items ORDER BY id LIMIT :limit")
    List<ItemEntity> findLimited(int limit);
}
