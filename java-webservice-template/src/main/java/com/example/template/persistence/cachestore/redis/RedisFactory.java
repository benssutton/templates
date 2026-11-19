package com.example.template.persistence.cachestore.redis;

import io.micronaut.context.annotation.Bean;
import io.micronaut.context.annotation.Factory;
import io.micronaut.context.annotation.Value;
import jakarta.inject.Singleton;
import java.net.URI;
import java.time.Duration;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import redis.clients.jedis.Connection;
import redis.clients.jedis.DefaultJedisClientConfig;
import redis.clients.jedis.RedisClient;
import redis.clients.jedis.util.JedisURIHelper;

/**
 * Provides the Jedis client (RedisJSON-capable; blocking, fine on virtual threads). The pool size
 * and every wait are bounded, so a frozen Redis turns into a prompt {@code
 * JedisConnectionException} (503) instead of a hung request.
 */
@Factory
public class RedisFactory {

    @Singleton
    @Bean(preDestroy = "close")
    public RedisClient redisClient(
            @Value("${redis.uri}") String uri,
            @Value("${redis.pool-size:16}") int poolSize,
            @Value("${redis.connect-timeout-ms:2000}") int connectTimeoutMs,
            @Value("${redis.socket-timeout-ms:2000}") int socketTimeoutMs) {
        URI u = URI.create(uri);
        DefaultJedisClientConfig.Builder config =
                DefaultJedisClientConfig.builder()
                        .user(JedisURIHelper.getUser(u))
                        .password(JedisURIHelper.getPassword(u))
                        .database(JedisURIHelper.getDBIndex(u))
                        .ssl(JedisURIHelper.isRedisSSLScheme(u))
                        .connectionTimeoutMillis(connectTimeoutMs)
                        .socketTimeoutMillis(socketTimeoutMs);
        GenericObjectPoolConfig<Connection> pool = new GenericObjectPoolConfig<>();
        pool.setMaxTotal(poolSize);
        pool.setMaxIdle(poolSize);
        // Waiting for a free connection is bounded like every other wait.
        pool.setMaxWait(Duration.ofMillis(connectTimeoutMs));
        return RedisClient.builder()
                .hostAndPort(JedisURIHelper.getHostAndPort(u))
                .clientConfig(config.build())
                .poolConfig(pool)
                .build();
    }
}
