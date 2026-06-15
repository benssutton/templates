package com.example.template.core;

import io.micronaut.http.context.ServerRequestContext;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

/**
 * Request-scoped per-boundary timings, rendered as a W3C Server-Timing header.
 * Held on the HttpRequest attribute so the handler thread (via
 * ServerRequestContext) appends samples and the response filter renders them.
 * Token names are normalised to match the k6 attribution parser ([A-Za-z0-9_]).
 */
public final class BoundarySamples {

    public static final String ATTR = "boundarySamples";
    private static final Pattern NON_TOKEN = Pattern.compile("[^A-Za-z0-9_]");

    private final List<Map.Entry<String, Double>> samples = new ArrayList<>();

    public synchronized void add(String label, double ms) {
        samples.add(Map.entry(label, ms));
    }

    /** Append a boundary sample to the current request's holder, if one is active. */
    public static void record(String label, double ms) {
        ServerRequestContext.currentRequest().ifPresent(req -> {
            Object holder = req.getAttribute(ATTR).orElse(null);
            if (holder instanceof BoundarySamples bs) {
                bs.add(label, ms);
            }
        });
    }

    /** Render samples (summing duplicate labels, first-seen order) plus a total. */
    public synchronized String render(double totalMs) {
        Map<String, Double> aggregated = new LinkedHashMap<>();
        for (Map.Entry<String, Double> e : samples) {
            String token = NON_TOKEN.matcher(e.getKey()).replaceAll("_");
            aggregated.merge(token, e.getValue(), Double::sum);
        }
        StringBuilder sb = new StringBuilder();
        for (Map.Entry<String, Double> e : aggregated.entrySet()) {
            sb.append(e.getKey()).append(";dur=").append(String.format("%.2f", e.getValue())).append(", ");
        }
        sb.append("total;dur=").append(String.format("%.2f", totalMs));
        return sb.toString();
    }
}
