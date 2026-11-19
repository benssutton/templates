package com.example.template.service;

import com.example.template.dto.CachedDataRow;
import com.example.template.dto.CachedDataRowsResponse;
import com.example.template.persistence.streamstore.LsmStore;
import jakarta.inject.Singleton;

/** Reads of the in-memory LSM store: the newest {@code limit} rows, oldest first. */
@Singleton
public class CachedDataService {

    private final LsmStore store;

    public CachedDataService(LsmStore store) {
        this.store = store;
    }

    public CachedDataRowsResponse query(int limit) {
        LsmStore.QueryResult result = store.query(limit);
        return new CachedDataRowsResponse(
                result.rows().stream()
                        .map(r -> new CachedDataRow(r.id(), r.name(), r.value(), r.seqno(), r.op()))
                        .toList(),
                result.total(),
                limit);
    }
}
