package com.example.template.service;

import com.example.template.dto.DataRow;
import com.example.template.dto.DataRowsResponse;
import com.example.template.persistence.streamstore.LsmStore;
import jakarta.inject.Singleton;

/**
 * Reads of the in-memory LSM store: the merged live view (newest version per key, no deletes), the
 * first {@code limit} by id.
 */
@Singleton
public class CachedDataService {

    private final LsmStore store;

    public CachedDataService(LsmStore store) {
        this.store = store;
    }

    public DataRowsResponse query(int limit) {
        LsmStore.QueryResult result = store.query(limit);
        return new DataRowsResponse(
                result.rows().stream().map(r -> new DataRow(r.id(), r.name(), r.value())).toList(),
                result.total(),
                limit);
    }
}
