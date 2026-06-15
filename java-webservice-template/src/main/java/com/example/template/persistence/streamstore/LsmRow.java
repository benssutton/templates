package com.example.template.persistence.streamstore;

public record LsmRow(long id, String name, String value, long seqno, String op) {}
