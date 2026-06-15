package com.example.template.service;

import com.example.template.dto.health.ProbeResult;
import com.example.template.health.DependencyHealthProbe;
import jakarta.inject.Singleton;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.JedisPooled;

/** Redis dependency probe (mirrors Python CacheService.health_check). */
@Singleton
public class CacheHealthProbe implements DependencyHealthProbe {

    private static final Logger LOG = LoggerFactory.getLogger(CacheHealthProbe.class);
    private final JedisPooled jedis;

    public CacheHealthProbe(JedisPooled jedis) {
        this.jedis = jedis;
    }

    @Override
    public String name() {
        return "redis";
    }

    @Override
    public ProbeResult probe() {
        long start = System.nanoTime();
        try {
            jedis.ping();
            return ProbeResult.up("redis", ms(start));
        } catch (Exception e) {
            LOG.error("redis health check failed: {}", e.toString());
            return ProbeResult.down("redis", ms(start), "unavailable");
        }
    }

    private static double ms(long startNanos) {
        return Math.round((System.nanoTime() - startNanos) / 1_000_000.0 * 100.0) / 100.0;
    }
}
