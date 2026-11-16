package com.example.template.persistence.cachestore.redis;

import io.micronaut.context.annotation.Factory;
import io.micronaut.context.annotation.Value;
import jakarta.inject.Singleton;
import redis.clients.jedis.RedisClient;

import java.net.URI;

/** Provides the Jedis client (RedisJSON-capable). Blocking, runs fine on virtual threads. */
@Factory
public class RedisFactory {

    @Singleton
    public RedisClient redisClient(@Value("${redis.uri:redis://localhost:6379/0}") String uri) {
        return RedisClient.create(URI.create(uri));
    }
}
