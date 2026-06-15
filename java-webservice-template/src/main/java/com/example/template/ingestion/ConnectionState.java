package com.example.template.ingestion;

public enum ConnectionState {
    CONNECTED("connected"),
    RECONNECTING("reconnecting"),
    DOWN("down");

    private final String value;

    ConnectionState(String value) {
        this.value = value;
    }

    public String value() {
        return value;
    }
}
