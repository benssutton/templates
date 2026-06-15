package com.example.template.observability;

/** Minimal mutable double a Micrometer gauge reads via a {@code ToDoubleFunction}. */
public final class MutableDouble {
    private volatile double value;

    public void set(double v) { this.value = v; }
    public double get() { return value; }
}
