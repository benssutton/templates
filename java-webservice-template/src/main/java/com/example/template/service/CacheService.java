package com.example.template.service;

import com.example.template.dto.CacheEntry;
import io.micronaut.json.JsonMapper;
import io.micronaut.json.tree.JsonNode;
import jakarta.inject.Singleton;
import java.io.IOException;
import java.io.UncheckedIOException;
import redis.clients.jedis.AbstractTransaction;
import redis.clients.jedis.RedisClient;
import redis.clients.jedis.Response;
import redis.clients.jedis.json.Path2;

/**
 * Redis cache over RedisJSON. Values travel as JSON text in both directions, written and read with
 * the service's own JSON mapper, so a value comes back exactly as it was sent (an integer stays an
 * integer; Jedis' Gson mapper would turn it into a double).
 */
@Singleton
public class CacheService {

    /** Redis TTL reply for a key that does not exist. */
    private static final long TTL_NO_KEY = -2;

    private final RedisClient jedis;
    private final JsonMapper json;

    public CacheService(RedisClient jedis, JsonMapper json) {
        this.jedis = jedis;
        this.json = json;
    }

    /**
     * JSON.SET and EXPIRE run in one MULTI/EXEC transaction, so a failure between them can never
     * leave a key that should expire stored forever. EXEC reports a failed command (e.g. WRONGTYPE)
     * as an element of its reply rather than throwing, so each {@link Response#get()} is read: it
     * rethrows that command's error.
     */
    public CacheEntry set(String key, JsonNode value, Integer ttlSeconds) {
        String text = write(value);
        try (AbstractTransaction tx = jedis.multi()) {
            // The Path2 overload sends the string as-is: it must already be JSON.
            Response<String> set = tx.jsonSet(key, Path2.ROOT_PATH, text);
            Response<Long> expire = ttlSeconds != null ? tx.expire(key, ttlSeconds) : null;
            tx.exec();
            set.get();
            if (expire != null) {
                expire.get();
            }
        }
        return new CacheEntry(key, value, ttlSeconds);
    }

    /**
     * Value and TTL are read in one transaction so they describe the same key state; a key that is
     * gone (or expired) by then is not found, never "no expiry".
     */
    public CacheEntry get(String key) {
        Response<Object> value;
        Response<Long> ttl;
        try (AbstractTransaction tx = jedis.multi()) {
            // With a JSONPath, RedisJSON answers an array of matches: [value].
            value = tx.jsonGet(key, Path2.ROOT_PATH);
            ttl = tx.ttl(key);
            tx.exec();
        }
        if (value.get() == null || ttl.get() == TTL_NO_KEY) {
            return null;
        }
        JsonNode matches = read(value.get().toString());
        Integer ttlSeconds = ttl.get() >= 0 ? ttl.get().intValue() : null;
        return new CacheEntry(key, matches.get(0), ttlSeconds);
    }

    private String write(JsonNode value) {
        try {
            return json.writeValueAsString(value == null ? JsonNode.nullNode() : value);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private JsonNode read(String text) {
        try {
            return json.readValue(text, JsonNode.class);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
