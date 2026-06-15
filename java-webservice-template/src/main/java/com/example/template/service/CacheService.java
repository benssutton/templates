package com.example.template.service;

import com.example.template.dto.CacheEntry;
import jakarta.inject.Singleton;
import redis.clients.jedis.JedisPooled;
import redis.clients.jedis.json.Path2;

/** Redis cache over RedisJSON (mirrors Python services/cache.py). */
@Singleton
public class CacheService {

    private final JedisPooled jedis;

    public CacheService(JedisPooled jedis) {
        this.jedis = jedis;
    }

    public CacheEntry set(String key, Object value, Integer ttlSeconds) {
        // jsonSetWithEscape routes the value through Jedis' JSON object mapper (Gson)
        // so a Map serialises to real JSON, not Java's Map.toString().
        jedis.jsonSetWithEscape(key, Path2.ROOT_PATH, value);
        if (ttlSeconds != null) {
            jedis.expire(key, ttlSeconds);
        }
        return new CacheEntry(key, value, ttlSeconds);
    }

    public CacheEntry get(String key) {
        Object value = jedis.jsonGet(key);
        if (value == null) {
            return null;
        }
        long ttl = jedis.ttl(key);
        Integer ttlSeconds = ttl >= 0 ? (int) ttl : null;
        return new CacheEntry(key, value, ttlSeconds);
    }
}
