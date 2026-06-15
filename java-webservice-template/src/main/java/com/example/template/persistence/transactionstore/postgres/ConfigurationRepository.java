package com.example.template.persistence.transactionstore.postgres;

import io.micronaut.data.annotation.Query;
import io.micronaut.data.jdbc.annotation.JdbcRepository;
import io.micronaut.data.model.query.builder.sql.Dialect;
import io.micronaut.data.repository.CrudRepository;

import java.util.List;

@JdbcRepository(dialect = Dialect.POSTGRES)
public interface ConfigurationRepository extends CrudRepository<ConfigurationEntity, String> {

    @Query("INSERT INTO configuration(key, value) VALUES (:key, :value) "
         + "ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value")
    void upsert(String key, String value);

    @Query("SELECT key, value FROM configuration ORDER BY key")
    List<ConfigurationEntity> findAllOrdered();
}
