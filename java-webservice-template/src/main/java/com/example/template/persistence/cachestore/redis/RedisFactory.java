package com.example.template.persistence.cachestore.redis;

import io.micronaut.context.annotation.Bean;
import io.micronaut.context.annotation.Factory;
import jakarta.inject.Singleton;
import java.net.URI;
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
    public RedisClient redisClient(RedisSettings settings) {
        URI u = URI.create(settings.uri());
        DefaultJedisClientConfig.Builder config =
                DefaultJedisClientConfig.builder()
                        .user(JedisURIHelper.getUser(u))
                        .password(JedisURIHelper.getPassword(u))
                        .database(JedisURIHelper.getDBIndex(u))
                        .ssl(JedisURIHelper.isRedisSSLScheme(u))
                        .connectionTimeoutMillis((int) settings.connectTimeout().toMillis())
                        .socketTimeoutMillis((int) settings.socketTimeout().toMillis());
        GenericObjectPoolConfig<Connection> pool = new GenericObjectPoolConfig<>();
        pool.setMaxTotal(settings.poolSize());
        pool.setMaxIdle(settings.poolSize());
        // Waiting for a free connection is bounded like every other wait.
        pool.setMaxWait(settings.connectTimeout());
        return RedisClient.builder()
                .hostAndPort(JedisURIHelper.getHostAndPort(u))
                .clientConfig(config.build())
                .poolConfig(pool)
                .build();
    }
}
