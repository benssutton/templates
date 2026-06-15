package com.example.template.service;

import com.example.template.core.Timed;
import com.example.template.dto.DataRow;
import com.example.template.dto.DataRowsResponse;
import com.example.template.persistence.analyticsstore.clickhouse.ItemEntity;
import com.example.template.persistence.analyticsstore.clickhouse.ItemRepository;
import jakarta.inject.Singleton;

import java.util.List;

/** ClickHouse analytics reads (mirrors Python services/data.py) with Server-Timing boundaries. */
@Singleton
public class DataService {

    private final ItemRepository repository;

    public DataService(ItemRepository repository) {
        this.repository = repository;
    }

    public DataRowsResponse getData(int limit) {
        long total;
        try (Timed t = Timed.start("clickhouse.count")) {
            total = repository.countAll();
        }
        List<ItemEntity> items;
        try (Timed t = Timed.start("clickhouse.select")) {
            items = repository.findLimited(limit);
        }
        List<DataRow> rows = items.stream()
            .map(i -> new DataRow(i.id(), i.name(), i.value()))
            .toList();
        return new DataRowsResponse(rows, total, limit);
    }
}
